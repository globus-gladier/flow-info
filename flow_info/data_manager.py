import datetime
import logging
import re
import pathlib
from flow_info.database import MonthDB
from flow_info.exc import MigrationRequired
from flow_info.legacy import LegacyMonths

log = logging.getLogger(__name__)


class DataManager:
    """Load and save beamline data, one file set per month.

    Flows, runs and run logs live in one SQLite database per month, and counts are
    selected straight from it. Months that haven't been migrated yet are still JSON, which can
    be read through self.legacy but not updated. See flow_info.migration.
    """

    DB_FILENAME = "{name}-{year_month}.db"

    def __init__(self, config):
        self.config = config
        self.name = self.config["beamlines"]["current_app"]
        self.basepath = pathlib.Path(self.config["beamlines"]["data_path"]).absolute()
        self.legacy = LegacyMonths(self.basepath, self.name)
        self._dbs = {}

    @property
    def db_filename_pattern(self):
        return rf"{self.name}-(?P<year>\d{{4}})-(?P<month>\d{{2}})\.db$"

    # Storage format of each month

    def has_db(self, year_month: str) -> bool:
        return year_month in self._dbs or self.get_db_filename(year_month).exists()

    def is_legacy(self, year_month: str) -> bool:
        """True if the month only exists as JSON."""
        return not self.has_db(year_month) and self.legacy.exists(year_month)

    def require_migrated(self, year_months):
        """Raise MigrationRequired if any of these months still need migrating."""
        legacy = [ym for ym in year_months if self.is_legacy(ym)]
        if legacy:
            raise MigrationRequired(legacy)

    def get_db(self, year_month: str) -> MonthDB:
        """The month's database, created if it doesn't exist."""
        if year_month not in self._dbs:
            self._dbs[year_month] = MonthDB(self.get_db_filename(year_month))
        return self._dbs[year_month]

    def close(self):
        for db in self._dbs.values():
            db.close()
        self._dbs = {}

    def get_available_runs(self):
        log.debug(f"Fetching available runs")
        months = set(self.legacy.get_available_months())
        for filename in self.basepath.iterdir():
            m = re.match(self.db_filename_pattern, filename.name)
            if m:
                months.add(f"{m.group('year')}-{m.group('month')}")
        return sorted(months)

    def has_legacy_flows(self, year_month: str) -> bool:
        """True if the month's database was migrated before flows were stored in it,
        and its flows are still only in JSON."""
        return (
            self.has_db(year_month)
            and self.legacy.get_flows_filename(year_month).exists()
            and self.get_db(year_month).count_flows() == 0
        )

    def needs_migration(self, year_month: str) -> bool:
        return self.is_legacy(year_month) or self.has_legacy_flows(year_month)

    # Flows

    def load_flows(self, year_month: str) -> list:
        if self.has_db(year_month) and not self.has_legacy_flows(year_month):
            return self.get_db(year_month).get_flows()
        return self.legacy.load_flows(year_month)

    def save_flows(self, year_month: str, flows: list):
        """Replace the month's flows. This creates the month's database if needed."""
        self.require_migrated([year_month])
        fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.get_db(year_month).save_flows(flows, fetched_at)

    # Runs

    def iter_runs(self, year_month: str):
        """Every run stored for the month, oldest first."""
        if self.has_db(year_month):
            yield from self.get_db(year_month).iter_runs()
        else:
            yield from self.legacy.load_runs(year_month)

    def save_runs(self, year_month: str, runs: list):
        self.require_migrated([year_month])
        self.get_db(year_month).save_runs(runs)

    # Run logs

    def get_run_log(self, year_month: str, run_id: str):
        return self.get_db(year_month).get_run_log(run_id)

    def get_run_logs(self, year_month: str, runs: list):
        """Yield (run_id, run_log) for each run, oldest first, run_log None if not stored."""
        runs = sorted(runs, key=lambda x: x["start_time"])
        if self.has_db(year_month):
            yield from self.get_db(year_month).get_run_logs([r["run_id"] for r in runs])
        else:
            yield from self.legacy.get_run_logs(year_month, runs)

    def save_run_log(self, year_month: str, run_id: str, run_log: dict):
        self.require_migrated([year_month])
        fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.get_db(year_month).save_run_logs([(run_id, run_log)], fetched_at)

    def runs_missing_logs(self, year_month: str) -> list:
        """(run_id, start_time, status) for runs without a stored log, oldest first."""
        if not self.has_db(year_month):
            return []
        return self.get_db(year_month).runs_missing_logs()

    def count_unfinished_runs(self, year_month: str) -> int:
        """Runs stored as ACTIVE or INACTIVE, whose status needs refreshing."""
        if not self.has_db(year_month):
            return 0
        return self.get_db(year_month).count_unfinished_runs()

    # Counts. A month with no data counts as empty, without creating a database.

    def count_runs(self, year_month: str) -> int:
        if self.has_db(year_month):
            return self.get_db(year_month).count_runs()
        if self.legacy.exists(year_month):
            return self.legacy.count_runs(year_month)
        return 0

    def count_run_logs(self, year_month: str) -> int:
        if self.has_db(year_month):
            return self.get_db(year_month).count_run_logs()
        if self.legacy.exists(year_month):
            return self.legacy.count_run_logs(year_month)
        return 0

    def count_missing_logs(self, year_month: str) -> int:
        if self.has_db(year_month):
            return self.get_db(year_month).count_missing_logs()
        return self.count_runs(year_month) - self.count_run_logs(year_month)

    # File sizes

    def get_size(self, filename: pathlib.Path):
        if not filename.exists():
            return 0
        return filename.stat().st_size

    def get_month_size(self, year_month: str) -> int:
        """Bytes on disk for the month's flows, runs and run logs."""
        if self.has_db(year_month):
            return self.get_size(self.get_db_filename(year_month))
        return self.legacy.get_month_size(year_month)

    def get_db_filename(self, year_month: str) -> pathlib.Path:
        return self.basepath / self.DB_FILENAME.format(name=self.name, year_month=year_month)
