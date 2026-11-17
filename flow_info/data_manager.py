import globus_sdk
import json
import logging
import os
import re
import pathlib

log = logging.getLogger(__name__)


class DataManager:

    RUNS_FILENAME = "{name}-{year_month}-runs.json"
    RUN_LOGS_FILENAME = "{name}-{year_month}-run-logs-{batch}.json"
    # Sidecar listing the run ids stored in a run logs file, so we can tell whether a
    # bucket needs anything fetched without parsing the (potentially multi-GB) file.
    RUN_LOG_IDS_FILENAME = "{name}-{year_month}-run-logs-{batch}.ids.json"
    FLOWS_FILENAME = "{name}-{year_month}-flows.json"
    CACHE_INFO_FILENAME = "cache_info.json"

    def __init__(self, config):
        self.config = config
        self.name = self.config["beamlines"]["current_app"]
        self.basepath = pathlib.Path(self.config["beamlines"]["data_path"]).absolute()
        self.cache_info = self.load_data(self.basepath / self.CACHE_INFO_FILENAME)
        if self.name not in self.cache_info:
            self.cache_info[self.name] = {}

    @property
    def runs_filename_pattern(self):
        return f"{self.name}-" + "(?P<year>\d{4})-(?P<month>\d{2})-runs.json"

    @property
    def run_logs_filename_pattern(self):
        return f"{self.name}-" + "(?P<year>\d{4})-(?P<month>\d{2})-run-logs-\d+.json"

    def load_flows(self, year_month: str):
        return self.load_data(self.get_filename(self.FLOWS_FILENAME, year_month))

    def load_runs(self, year_month: str):
        return self.load_data(self.get_filename(self.RUNS_FILENAME, year_month))

    def load_run_logs(self, year_month: str, batch: int):
        return self.load_data(
            self.get_filename(self.RUN_LOGS_FILENAME, year_month, batch=batch)
        )

    def load_run_log_ids(self, year_month: str, batch: int):
        """Return the set of run ids stored in a run logs file.

        Returns None if the logs file exists but has no index yet, in which case the
        caller needs to load the file to build one.
        """
        ids_path = self.get_filename(self.RUN_LOG_IDS_FILENAME, year_month, batch=batch)
        if ids_path.exists():
            return set(self.load_data(ids_path))
        if not self.get_filename(self.RUN_LOGS_FILENAME, year_month, batch=batch).exists():
            return set()
        return None

    def save_run_log_ids(self, year_month: str, batch: int, run_ids):
        path = self.get_filename(self.RUN_LOG_IDS_FILENAME, year_month, batch=batch)
        self.save_data(path, sorted(run_ids))

    def save_cache_info(self):
        self.save_data(self.basepath / self.CACHE_INFO_FILENAME, self.cache_info, indent=2)

    def save_flows(self, year_month: str, data: dict):
        self.save_data(self.get_filename(self.FLOWS_FILENAME, year_month), data, indent=2)

    def save_runs(self, year_month: str, data: dict):
        if not self.cache_info[self.name].get(year_month):
            self.cache_info[self.name][year_month] = {}
        self.cache_info[self.name][year_month]["runs"] = len(data["runs"])
        self.save_cache_info()
        self.save_data(self.get_filename(self.RUNS_FILENAME, year_month), data)

    def save_run_logs(self, year_month: str, data: dict, batch: int):
        if not data.get("logs"):
            # Don't save empty log files
            return
        if not self.cache_info[self.name].get(year_month):
            self.cache_info[self.name][year_month] = {}
        if not self.cache_info[self.name][year_month].get("logs"):
            self.cache_info[self.name][year_month]["logs"] = {}
        self.cache_info[self.name][year_month]["logs"][str(batch)] = len(data.get("logs", []))
        self.save_cache_info()
        self.save_data(
            self.get_filename(self.RUN_LOGS_FILENAME, year_month, batch=batch), data
        )
        self.save_run_log_ids(year_month, batch, data["logs"].keys())

    def get_available_runs(self):
        log.debug(f"Fetching available runs")

        basepath = pathlib.Path(self.config["beamlines"]["data_path"]).absolute()
        matches = [
            re.match(self.runs_filename_pattern, filename.name)
            for filename in basepath.iterdir()
        ]
        return [f"{m.group('year')}-{m.group('month')}" for m in matches if m]

    def get_size(self, filename: pathlib.Path):
        if not filename.exists():
            return 0
        return filename.stat().st_size

    def get_runs_file_size(self, year_month: str):
        return self.get_size(self.get_filename(self.RUNS_FILENAME, year_month))

    def get_run_logs_file_size(self, year_month: str):
        basepath = pathlib.Path(self.config["beamlines"]["data_path"]).absolute()
        pattern = f"{self.name}-{year_month}" + "-run-logs-\d+.json"
        matched_filenames = [
            filename for filename in basepath.iterdir()
            if re.match(pattern, filename.name)
        ]
        return sum([self.get_size(f) for f in matched_filenames])

    def get_filename(self, log_filename: str, year_month: str, batch=None):
        basepath = pathlib.Path(self.config["beamlines"]["data_path"])
        format_items = dict(name=self.name, year_month=year_month)
        if batch is not None:
            format_items["batch"] = batch
        filename = log_filename.format(**format_items)
        return basepath / filename

    def load_data(self, path):
        if not path.exists():
            log.debug(f"No file exists, '{path}")
            return {}
        log.debug(f"Loading {path}...")
        if path.stat().st_size == 0:
            return {}
        # json.load parses straight from the file, rather than holding the whole
        # text in memory alongside the parsed result.
        with open(path) as f:
            return json.load(f)

    def save_data(self, path, data, indent=None):
        log.debug(f"Saving: {path}")
        # Stream to a temp file, then swap it in. This avoids building the whole
        # serialized text in memory, and a crash mid-write can't corrupt the existing file.
        tmp_path = path.with_name(path.name + ".tmp")
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=indent)
        os.replace(tmp_path, path)
