"""Read-only access to months that are still stored as JSON.

A legacy month is a flows file, a runs file and run logs bucket files of BUCKET_SIZE
runs each.
Parsing them is slow, so run and log counts are kept in cache_info.json for summary.
Nothing here is used for migrated months, and the whole module can go once every
month is migrated. See flow_info.migration.
"""

import json
import logging
import os
import pathlib
import re

log = logging.getLogger(__name__)


class LegacyMonths:

    FLOWS_FILENAME = "{name}-{year_month}-flows.json"
    RUNS_FILENAME = "{name}-{year_month}-runs.json"
    RUN_LOGS_FILENAME = "{name}-{year_month}-run-logs-{batch}.json"
    CACHE_INFO_FILENAME = "cache_info.json"
    # Runs per run logs bucket file.
    BUCKET_SIZE = 2500

    def __init__(self, basepath: pathlib.Path, name: str):
        self.basepath = basepath
        self.name = name
        self._cache_info = None

    @property
    def runs_filename_pattern(self):
        return rf"{self.name}-(?P<year>\d{{4}})-(?P<month>\d{{2}})-runs\.json$"

    def run_logs_filename_pattern(self, year_month: str):
        return rf"{self.name}-{year_month}-run-logs-(?P<batch>\d+)\.json$"

    # Files

    def get_flows_filename(self, year_month: str) -> pathlib.Path:
        return self.basepath / self.FLOWS_FILENAME.format(name=self.name, year_month=year_month)

    def get_runs_filename(self, year_month: str) -> pathlib.Path:
        return self.basepath / self.RUNS_FILENAME.format(name=self.name, year_month=year_month)

    def get_run_logs_filename(self, year_month: str, batch: int) -> pathlib.Path:
        return self.basepath / self.RUN_LOGS_FILENAME.format(
            name=self.name, year_month=year_month, batch=batch
        )

    def get_run_logs_filenames(self, year_month: str) -> list:
        """The month's run logs buckets, in bucket order."""
        pattern = self.run_logs_filename_pattern(year_month)
        buckets = []
        for filename in self.basepath.iterdir():
            m = re.match(pattern, filename.name)
            if m:
                buckets.append((int(m.group("batch")), filename))
        return [filename for _, filename in sorted(buckets)]

    def exists(self, year_month: str) -> bool:
        return self.get_runs_filename(year_month).exists()

    def get_available_months(self) -> list:
        months = set()
        for filename in self.basepath.iterdir():
            m = re.match(self.runs_filename_pattern, filename.name)
            if m:
                months.add(f"{m.group('year')}-{m.group('month')}")
        return sorted(months)

    def get_month_files(self, year_month: str) -> list:
        """The month's JSON files that exist: flows, runs and run logs buckets."""
        paths = [self.get_flows_filename(year_month), self.get_runs_filename(year_month)]
        return [p for p in paths if p.exists()] + self.get_run_logs_filenames(year_month)

    def get_month_size(self, year_month: str) -> int:
        return sum(p.stat().st_size for p in self.get_month_files(year_month))

    # Flows, runs and run logs

    def load_flows(self, year_month: str) -> list:
        return load_json(self.get_flows_filename(year_month)).get("flows", [])

    def load_runs(self, year_month: str) -> list:
        return load_json(self.get_runs_filename(year_month)).get("runs", [])

    def partition_buckets(self, runs: list):
        """(bucket_num, runs) for each bucket file. runs must be sorted by start_time."""
        return enumerate(
            runs[i : i + self.BUCKET_SIZE] for i in range(0, len(runs), self.BUCKET_SIZE)
        )

    def get_run_logs(self, year_month: str, runs: list):
        """Yield (run_id, run_log) for each run, oldest first, run_log None if not stored.

        Logs are found by bucket position, so runs must be every run in the month.
        """
        runs = sorted(runs, key=lambda x: x["start_time"])
        for bucket_num, bucket in self.partition_buckets(runs):
            logs = load_json(self.get_run_logs_filename(year_month, bucket_num)).get("logs", {})
            for run in bucket:
                yield run["run_id"], logs.get(run["run_id"])

    # Counts, from cache_info.json

    @property
    def cache_info(self) -> dict:
        """Counts per month, as {name: {year_month: {"runs": n, "logs": {bucket: n}}}}."""
        if self._cache_info is None:
            self._cache_info = load_json(self.basepath / self.CACHE_INFO_FILENAME)
            self._cache_info.setdefault(self.name, {})
        return self._cache_info

    def count_runs(self, year_month: str) -> int:
        info = self.cache_info[self.name].get(year_month, {})
        if "runs" in info:
            return info["runs"]
        return len(self.load_runs(year_month))

    def count_run_logs(self, year_month: str) -> int:
        """Stored logs. 0 if the month's logs haven't been counted, see refresh_counts."""
        return sum(self.cache_info[self.name].get(year_month, {}).get("logs", {}).values())

    def refresh_counts(self):
        """Recount runs and logs for every legacy month and save cache_info.json."""
        for year_month in self.get_available_months():
            runs = sorted(self.load_runs(year_month), key=lambda x: x["start_time"])
            log_counts = {}
            for bucket_num, _ in self.partition_buckets(runs):
                logs = load_json(self.get_run_logs_filename(year_month, bucket_num))
                log_counts[str(bucket_num)] = len(logs.get("logs", {}))
            self.cache_info[self.name][year_month] = {"runs": len(runs), "logs": log_counts}
        save_json(self.basepath / self.CACHE_INFO_FILENAME, self.cache_info)


def load_json(path: pathlib.Path) -> dict:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    log.debug(f"Loading {path}...")
    with open(path) as f:
        return json.load(f)


def save_json(path: pathlib.Path, data: dict):
    log.debug(f"Saving: {path}")
    tmp_path = path.with_name(path.name + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)
