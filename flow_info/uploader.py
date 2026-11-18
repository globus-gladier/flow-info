import contextlib
import dataclasses
import datetime
import fnmatch
import logging
import os
import pathlib
import urllib.parse

import globus_sdk

from flow_info.exc import ConfigException

log = logging.getLogger(__name__)


def parse_globus_url(url: str):
    """Split "globus://<collection_id>/<path>/" into (collection_id, path)."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "globus" or not parsed.netloc:
        raise ConfigException(
            f"remote_collection must look like globus://<collection_id>/<path>/, got {url!r}"
        )
    path = parsed.path or "/"
    if not path.endswith("/"):
        path += "/"
    return parsed.netloc, path


def to_gcp_path(filename: pathlib.Path) -> str:
    """Convert a local absolute path into the form Globus Connect Personal expects."""
    filename = filename.absolute()
    if os.name == "nt":
        # GCP on Windows exposes C:\Users\... as /C/Users/...
        return "/" + filename.drive.rstrip(":") + filename.as_posix()[len(filename.drive):]
    return filename.as_posix()


WEBAPP_TASK_URL = "https://app.globus.org/activity/{task_id}"
# Partially written files from an in-progress or crashed save.
EXCLUDE_PATTERNS = ["*.tmp"]


@dataclasses.dataclass
class PendingFile:
    """A local file the sync will send, and why."""

    path: pathlib.Path
    size: int
    reason: str  # "new" or "newer"


@dataclasses.dataclass
class SyncPlan:
    """What a sync of a local folder to the remote collection is expected to do."""

    pending: list[PendingFile]
    # Every local file the sync considers, synced or not.
    local_files: int
    # Remote files with no local copy. The sync leaves these alone.
    remote_only: list[str]

    @property
    def pending_size(self) -> int:
        return sum(item.size for item in self.pending)


@contextlib.contextmanager
def consent_hint(collection_id: str):
    """Turn ConsentRequired errors into a hint about remote_data_access."""
    try:
        yield
    except globus_sdk.TransferAPIError as e:
        if e.info.consent_required:
            raise ConfigException(
                f"{collection_id} requires data_access consent. Set "
                "remote_data_access = True in beamlines.cfg and try again."
            ) from e
        raise


class Uploader:
    """Sync the local data folder to a Globus collection with a Globus Transfer task.

    The source is the local Globus Connect Personal endpoint, found automatically with
    LocalGlobusConnectPersonal, unless `local_collection` is set in the config.
    """

    def __init__(
        self,
        app: globus_sdk.GlobusApp,
        remote_collection: str,
        local_collection: str = None,
        remote_data_access: bool = False,
    ):
        self.app = app
        self.collection_id, self.base_path = parse_globus_url(remote_collection)
        self.local_collection = local_collection or self.find_local_endpoint()
        # This registers the transfer scope on the app, so it must happen before login.
        self.transfer_client = globus_sdk.TransferClient(app=self.app)
        if remote_data_access:
            # Only for non-High-Assurance GCS mapped collections, see
            # TransferClient.add_app_data_access_scope.
            self.transfer_client.add_app_data_access_scope(self.collection_id)

    def login(self):
        """Log in the upload app, a no-op if it's the main app and already logged in."""
        self.app.login()

    def find_local_endpoint(self) -> str:
        endpoint_id = globus_sdk.LocalGlobusConnectPersonal().endpoint_id
        if not endpoint_id:
            raise ConfigException(
                "No local Globus Connect Personal endpoint was found. Install and run "
                "Globus Connect Personal, or set local_collection = \"<collection_id>\" "
                "in beamlines.cfg."
            )
        log.debug(f"Found local Globus Connect Personal endpoint {endpoint_id}")
        return endpoint_id

    def check_local_endpoint(self):
        """Fail early if GCP is not connected, since the task would sit waiting forever."""
        endpoint = self.transfer_client.get_endpoint(self.local_collection)
        if endpoint.get("is_globus_connect") and not endpoint.get("gcp_connected"):
            raise ConfigException(
                f"Globus Connect Personal ({endpoint.get('display_name')}) is not "
                "connected. Start it and try again."
            )

    def list_remote_dir(self, path: str) -> dict:
        """Map of name -> ls entry for a remote directory, empty if it doesn't exist."""
        try:
            with consent_hint(self.collection_id):
                return {
                    item["name"]: item
                    for item in self.transfer_client.operation_ls(self.collection_id, path)
                }
        except globus_sdk.TransferAPIError as e:
            if e.http_status == 404:
                return {}
            raise

    def get_collection_name(self, collection_id: str) -> str:
        return self.transfer_client.get_endpoint(collection_id).get("display_name") or collection_id

    def plan_sync(self, local_dir: pathlib.Path) -> SyncPlan:
        """Compare local_dir with the remote path, using the same rule as an mtime sync:
        a file is sent if it's missing remotely or newer locally.

        This is an estimate, Globus Transfer makes the final decision when the task runs.
        """
        pending, local_files, remote_only = [], 0, []
        for dirpath, _, filenames in os.walk(local_dir):
            rel_dir = pathlib.Path(dirpath).relative_to(local_dir)
            remote_dir = f"{self.base_path}{rel_dir.as_posix()}/".replace("/./", "/")
            remote = self.list_remote_dir(remote_dir)
            names = [
                name
                for name in sorted(filenames)
                if not any(fnmatch.fnmatch(name, pat) for pat in EXCLUDE_PATTERNS)
            ]
            remote_only += [
                (rel_dir / name).as_posix()
                for name, entry in sorted(remote.items())
                if entry.get("type") == "file" and name not in names
            ]
            for name in names:
                local_files += 1
                path = pathlib.Path(dirpath) / name
                stat = path.stat()
                entry = remote.get(name)
                if entry is None:
                    pending.append(PendingFile(path, stat.st_size, "new"))
                    continue
                remote_mtime = datetime.datetime.fromisoformat(entry["last_modified"])
                # Transfer timestamps have one-second resolution.
                if int(stat.st_mtime) > remote_mtime.timestamp():
                    pending.append(PendingFile(path, stat.st_size, "newer"))
        return SyncPlan(pending, local_files, remote_only)

    def submit(self, local_dir: pathlib.Path, label: str = "flow-info upload") -> str:
        """Submit a transfer task syncing local_dir to the remote path, returning the task id.

        This doesn't wait for the transfer, follow it with task_url().
        """
        self.check_local_endpoint()
        tdata = globus_sdk.TransferData(
            source_endpoint=self.local_collection,
            destination_endpoint=self.collection_id,
            label=label,
            # Only send files that are new or newer than the remote copy. Preserving
            # timestamps keeps the remote mtimes comparable on the next sync.
            sync_level="mtime",
            preserve_timestamp=True,
        )
        for pattern in EXCLUDE_PATTERNS:
            tdata.add_filter_rule(pattern, method="exclude", type="file")
        tdata.add_item(to_gcp_path(local_dir) + "/", self.base_path, recursive=True)
        with consent_hint(self.collection_id):
            task = self.transfer_client.submit_transfer(tdata)
        log.info(f"Submitted transfer task {task['task_id']}")
        return task["task_id"]

    @staticmethod
    def task_url(task_id: str) -> str:
        return WEBAPP_TASK_URL.format(task_id=task_id)
