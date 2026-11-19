"""Convert legacy JSON months (a runs file plus run logs buckets) to SQLite.

The JSON is streamed with ijson, so memory use stays at about one run log no matter
how large the bucket files are. The JSON files are left in place.
"""

import dataclasses
import datetime
import logging
import os

import ijson

from flow_info.data_manager import DataManager
from flow_info.database import MonthDB

log = logging.getLogger(__name__)

# Rows per insert. Logs are large, so they get smaller chunks.
RUNS_CHUNK = 1000
LOGS_CHUNK = 50


@dataclasses.dataclass
class MigrationResult:
    year_month: str
    flows: int
    runs: int
    run_logs: int
    json_size: int
    db_size: int


class MigrationError(Exception):
    pass


def chunked(items, size):
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def file_time(path) -> str:
    """The file's mtime as an ISO timestamp, the closest to when its data was fetched."""
    return datetime.datetime.fromtimestamp(
        path.stat().st_mtime, datetime.timezone.utc
    ).isoformat()


def migrate_flows(data_manager: DataManager, year_month: str, db: MonthDB) -> int:
    """Copy the month's JSON flows into db, if there are any. Returns the flow count."""
    flows_path = data_manager.legacy.get_flows_filename(year_month)
    if not flows_path.exists():
        return 0
    # Flows files are small enough to load whole.
    flows = data_manager.legacy.load_flows(year_month)
    db.save_flows(flows, file_time(flows_path))
    expected = len({f["id"] for f in flows})
    if db.count_flows() != expected:
        raise MigrationError(
            f"{year_month}: database has {db.count_flows()} flows, expected {expected}"
        )
    return expected


def migrate_month(data_manager: DataManager, year_month: str, callback=None) -> MigrationResult:
    """Migrate one JSON month to SQLite.

    The database is built under a temporary name and only moved into place once its
    counts match the JSON, so an interrupted migration leaves the month as JSON.

    A month migrated before flows were stored in the database only has its flows copied.

    :param callback: Called as callback(message) with progress updates.
    """
    callback = callback or (lambda message: None)
    if data_manager.has_legacy_flows(year_month):
        return migrate_month_flows(data_manager, year_month, callback)
    if data_manager.has_db(year_month):
        raise MigrationError(f"{year_month} is already migrated")
    if not data_manager.is_legacy(year_month):
        raise MigrationError(f"{year_month} has no JSON data to migrate")

    db_path = data_manager.get_db_filename(year_month)
    tmp_path = db_path.with_name(db_path.name + ".tmp")
    # Leftovers from an interrupted migration.
    for leftover in (tmp_path, tmp_path.with_name(tmp_path.name + "-journal")):
        leftover.unlink(missing_ok=True)

    runs_path = data_manager.legacy.get_runs_filename(year_month)
    log_paths = data_manager.legacy.get_run_logs_filenames(year_month)
    db = MonthDB(tmp_path)
    try:
        num_flows = migrate_flows(data_manager, year_month, db)
        callback(f"{year_month}: {num_flows} flows")

        run_ids = set()
        with open(runs_path, "rb") as f:
            # ijson returns non-integer numbers as Decimal, which the database encodes
            # as floats. (Its use_float option overflows on integers above 2**63.)
            for chunk in chunked(ijson.items(f, "runs.item"), RUNS_CHUNK):
                db.save_runs(chunk)
                run_ids.update(r["run_id"] for r in chunk)
        callback(f"{year_month}: {len(run_ids)} runs")

        log_ids = set()
        for idx, log_path in enumerate(log_paths):
            # The original fetch time isn't recorded, the file's mtime is the closest.
            fetched_at = file_time(log_path)
            with open(log_path, "rb") as f:
                for chunk in chunked(ijson.kvitems(f, "logs"), LOGS_CHUNK):
                    db.save_run_logs(chunk, fetched_at)
                    log_ids.update(run_id for run_id, _ in chunk)
            callback(
                f"{year_month}: run logs file {idx + 1}/{len(log_paths)}, {len(log_ids)} logs"
            )

        if db.count_runs() != len(run_ids) or db.count_run_logs() != len(log_ids):
            raise MigrationError(
                f"{year_month}: database has {db.count_runs()} runs and "
                f"{db.count_run_logs()} logs, expected {len(run_ids)} and {len(log_ids)}"
            )
    except BaseException:
        db.close()
        tmp_path.unlink(missing_ok=True)
        raise
    db.close()
    os.replace(tmp_path, db_path)

    json_size = data_manager.legacy.get_month_size(year_month)
    return MigrationResult(
        year_month,
        num_flows,
        len(run_ids),
        len(log_ids),
        json_size,
        data_manager.get_size(db_path),
    )


def migrate_month_flows(data_manager: DataManager, year_month: str, callback) -> MigrationResult:
    """Copy JSON flows into a month that's already migrated. The copy is one
    transaction, so an interruption leaves the database without flows."""
    db = data_manager.get_db(year_month)
    num_flows = migrate_flows(data_manager, year_month, db)
    callback(f"{year_month}: runs and logs already migrated, copied {num_flows} flows")
    return MigrationResult(
        year_month,
        num_flows,
        db.count_runs(),
        db.count_run_logs(),
        data_manager.legacy.get_month_size(year_month),
        data_manager.get_month_size(year_month),
    )
