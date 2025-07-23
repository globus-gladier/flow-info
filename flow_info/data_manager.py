import globus_sdk
import json
import logging
import re
import pathlib

log = logging.getLogger(__name__)


class DataManager:

    RUNS_FILENAME = "{name}-{year_month}-runs.json"
    RUN_LOGS_FILENAME = "{name}-{year_month}-run-logs.json"

    def __init__(self, config, name):
        self.config = config
        self.name = name

    @property
    def runs_filename_pattern(self):
        return f"{self.name}-" + "(?P<year>\d{4})-(?P<month>\d{2})-runs.json"

    @property
    def run_logs_filename_pattern(self):
        return f"{self.name}-" + "(?P<year>\d{4})-(?P<month>\d{2})-run-logs.json"

    def load_runs(self, year_month: str):
        if isinstance(year_month, list):
            raise ValueError(f"Received {year_month} as str, expected {[year_month]}")
        return self.load_data(self.get_filename(self.RUNS_FILENAME, year_month))

    def load_run_logs(self, year_month: str):
        if isinstance(year_month, list):
            raise ValueError(f"Received {year_month} as str, expected {[year_month]}")

        return self.load_data(self.get_filename(self.RUN_LOGS_FILENAME, year_month))

    def save_runs(self, year_month: str, data: dict):
        return self.load_data(self.get_filename(self.RUNS_FILENAME, year_month))

    def save_run_logs(self, year_month: str, data: dict):
        return self.save_data(
            self.get_filename(self.RUN_LOGS_FILENAME, year_month), data
        )

    def get_available_runs(self):
        basepath = pathlib.Path(self.config["beamlines"][self.name]["path"])
        matches = [
            re.match(self.runs_filename_pattern, filename.name)
            for filename in basepath.iterdir()
        ]
        return [f"{m.group('year')}-{m.group('month')}" for m in matches if m]

    def get_available_run_logs(self):
        pass

    def get_filename(self, log_filename: str, year_month: str):
        basepath = pathlib.Path(self.config["beamlines"][self.name]["path"])
        filename = log_filename.format(name=self.name, year_month=year_month)
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
