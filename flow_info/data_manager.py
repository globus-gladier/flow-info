import globus_sdk
import json
import logging
import re
import pathlib

log = logging.getLogger(__name__)


class DataManager:

    RUNS_FILENAME = "{name}-{year_month}-runs.json"
    RUN_LOGS_FILENAME = "{name}-{year_month}-run-logs-{batch}.json"
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

    def save_cache_info(self):
        self.save_data(self.basepath / self.CACHE_INFO_FILENAME, self.cache_info)

    def save_flows(self, year_month: str, data: dict):
        self.save_data(self.get_filename(self.FLOWS_FILENAME, year_month), data)

    def save_runs(self, year_month: str, data: dict):
        if not self.cache_info["xpcs"].get(year_month):
            self.cache_info["xpcs"][year_month] = {}
        self.cache_info["xpcs"][year_month]["runs"] = len(data["runs"])
        self.save_cache_info()
        self.save_data(self.get_filename(self.RUNS_FILENAME, year_month), data)

    def save_run_logs(self, year_month: str, data: dict, batch: int):
        if not data.get("logs"):
            # Don't save empty log files
            return
        if not self.cache_info["xpcs"].get(year_month):
            self.cache_info["xpcs"][year_month] = {}
        if not self.cache_info["xpcs"][year_month].get("logs"):
            self.cache_info["xpcs"][year_month]["logs"] = {}
        self.cache_info["xpcs"][year_month]["logs"][str(batch)] = len(
            data.get("logs", [])
        )
        self.save_cache_info()
        self.save_data(
            self.get_filename(self.RUN_LOGS_FILENAME, year_month, batch=batch), data
        )

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
            filename
            for filename in basepath.iterdir()
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
        with open(path) as f:
            data = f.read()
            if data:
                return json.loads(data)
            return {}

    def save_data(self, path, data):
        log.debug(f"Saving: {path}")
        with open(path, "w") as f:
            f.write(json.dumps(data, indent=2))
