import datetime
import json
import os
import pathlib
import configobj
import logging
import functools
import globus_sdk
import flow_info
from flow_info.exc import ConfigException
from flow_info.runs_cache import RunsCache
from flow_info.run_logs_cache import RunLogsCache
from flow_info.data_manager import DataManager
from flow_info.uploader import Uploader


log = logging.getLogger(__name__)


class FlowsCache:
    def __init__(self, config: configobj.ConfigObj):

        self.name = config["beamlines"]["current_app"]
        self.config = config
        if self.config is None:
            raise ValueError("Config can't be none!")
        self.app = self.get_client_app()
        # Share one DataManager, so each month's database is only opened once.
        self.data_manager = DataManager(self.config)
        self.runs_cache = RunsCache(self.app, self.config, self.data_manager)
        self.run_logs_cache = RunLogsCache(self.app, self.config, self.data_manager)

    def login(self):
        self.app.login()

    def get_client_app(
        self,
        client_type: str = None,
        client_id: str = None,
        app_name: str = None,
    ):
        beamline = self.config["beamlines"][self.name]
        client_type = client_type or beamline.get("client_type")
        client_id = client_id or beamline["client_id"]
        app_name = app_name or f"FlowInfo-{self.name}"

        if client_type == "user":
            return globus_sdk.UserApp(app_name=app_name, client_id=client_id)

        key = f"{self.name.upper()}_CLIENT_SECRET"
        secret = os.getenv(key)
        if not secret:
            raise ConfigException(f'Please set {key} to fetch data for client: export {key}="<secret>"')

        app = globus_sdk.ClientApp(
            app_name=app_name,
            client_id=client_id,
            client_secret=secret,
        )
        return app

    def get_upload_app(self):
        """The app used for uploads, set by remote_client_type.

        It defaults to the beamline's client_type, in which case the main app (and its
        login) is reused. A user upload for a client-credentials beamline logs in with
        the built-in flow-info client.
        """
        beamline = self.config["beamlines"][self.name]
        client_type = beamline.get("remote_client_type", beamline.get("client_type"))
        if client_type not in ("user", "client-credentials"):
            raise ConfigException(
                f'remote_client_type must be "user" or "client-credentials", got {client_type!r}'
            )
        if client_type == beamline.get("client_type"):
            return self.app
        if client_type == "client-credentials":
            # The beamline's client_id is a native client, which can't do client credentials.
            raise ConfigException(
                f'remote_client_type = "client-credentials" needs a client-credentials '
                f'beamline, but [[ {self.name} ]] uses client_type = "user"'
            )
        return self.get_client_app(
            client_type="user",
            client_id=flow_info.CLIENT_ID,
            # A separate name keeps its tokens apart from the main app's.
            app_name=f"FlowInfo-{self.name}-upload",
        )

    def get_flows_client(self):
        return globus_sdk.FlowsClient(app=self.get_client_app())

    def get_flows(self, year_month: str = None):
        if not year_month:
            year_month = self._get_year_month_now()
        return self.data_manager.load_flows(year_month)

    def get_runs(self, year_months: list = []):
        if not year_months:
            year_months = self.get_available_caches()
        return self.runs_cache.get_runs(year_months)

    def _to_year_month(self, timestamp):
        dt = datetime.datetime.fromisoformat(timestamp)
        return f"{dt.year}-{str(dt.month).zfill(2)}"

    def _get_year_month_now(self):
        now = datetime.datetime.now()
        return f"{now.year}-{str(now.month).zfill(2)}"

    def get_run_logs(self, runs: list, year_months: list = None):
        if year_months is None:
            year_months = sorted(
                list({self._to_year_month(r["start_time"]) for r in runs})
            )
        log.debug(f"Fetching year months for runs: {year_months}")
        for year_month in year_months:
            month_runs = [r for r in runs if self._to_year_month(r["start_time"]) == year_month]
            yield from self.run_logs_cache.get_run_logs(month_runs, year_month)

    def get_flow(self, flow_id: str):
        log.debug(f"Looking up flow {flow_id}")
        for flow in self.get_flows():
            if flow["flow_id"] == flow_id:
                return flow

    def get_available_caches(self):
        return sorted(self.data_manager.get_available_runs())

    def update_runs(self):
        yield from self.runs_cache.update_runs()

    def get_uploader(self) -> Uploader:
        """Create an Uploader for the configured remote_collection. Call before login()."""
        remote_collection = self.config["beamlines"][self.name].get("remote_collection")
        if not remote_collection:
            raise ConfigException(
                f'Set remote_collection = "globus://<collection_id>/<path>/" '
                f'under [[ {self.name} ]] to use flow-info upload'
            )
        beamline = self.config["beamlines"][self.name]
        return Uploader(
            self.get_upload_app(),
            remote_collection,
            local_collection=beamline.get("local_collection"),
            remote_data_access=beamline.as_bool("remote_data_access")
            if "remote_data_access" in beamline
            else False,
        )

    def update_flows(self, limit=0):
        flows_client = self.get_flows_client()
        kwargs = {}
        user_lookup = self.config["beamlines"][self.name]["client_type"] == "user"
        if user_lookup:
            kwargs["filter_role"] = "filter_viewer"
        flows = list(
            flows_client.paginated.list_flows(filter_role="flow_viewer").items()
        )
        owner = self.config["beamlines"][self.name].get("flow_owner")
        if owner:
            last = len(flows)
            flows = [f for f in flows if f["flow_owner"] == owner]
            log.info(f"Filtered {last} user flows to {len(flows)}")
        log.info(f"Fetched {len(flows)} Flows from service.")
        self.data_manager.save_flows(self._get_year_month_now(), flows)

    def get_log_update_months(self) -> list:
        """Months that can still have run logs to fetch. Logs are deleted 90 days after
        a run starts, so this is at most the last four months."""
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=90)
        months = []
        for year_month in self.get_available_caches():
            year, month = map(int, year_month.split("-"))
            next_month = datetime.datetime(
                year + month // 12, month % 12 + 1, 1, tzinfo=datetime.timezone.utc
            )
            if next_month > cutoff:
                months.append(year_month)
        return months

    def update_run_logs(self, callback, workers: int) -> int:
        """Fetch run logs for every month that can still have them.

        :param callback: Called as callback(year_month, done, total), where done and
            total count logs across all months and year_month is the month being
            fetched. It's called once with year_month None before fetching starts.
        Returns the number of logs that were due to be fetched.
        """
        months = self.get_log_update_months()
        # Fail before fetching anything if a month needs migrating first.
        self.data_manager.require_migrated(months)
        pending = {ym: self.run_logs_cache.get_pending_run_logs(ym) for ym in months}
        total = sum(len(run_ids) for run_ids in pending.values())
        callback(None, 0, total)
        done = 0
        for year_month, run_ids in pending.items():
            if not run_ids:
                continue
            log.debug(f"Fetching {len(run_ids)} {year_month} run logs...")
            self.run_logs_cache.update_run_logs(
                year_month,
                run_ids,
                lambda n: callback(year_month, done + n, total),
                workers,
            )
            done += len(run_ids)
        return total

    def _storage(self, year_month: str) -> str:
        if self.data_manager.is_legacy(year_month):
            return "json"
        if self.data_manager.has_legacy_flows(year_month):
            # Migrated before flows were stored in the database.
            return "sqlite, json flows"
        return "sqlite"

    def summary(self, year_months: list = None) -> list:
        dm = self.data_manager
        return [
            {
                "name": self.name,
                "year_month": year_month,
                "flows": len(self.get_flows(year_month)),
                "runs": dm.count_runs(year_month),
                "run_logs": dm.count_run_logs(year_month),
                "missing_logs": dm.count_missing_logs(year_month),
                "size": dm.get_month_size(year_month),
                "storage": self._storage(year_month),
            }
            for year_month in year_months or self.get_available_caches()
        ]
