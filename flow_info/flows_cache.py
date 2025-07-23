import datetime
import json
import os
import pathlib
import configobj
import logging
import functools
import globus_sdk
from flow_info.exc import ConfigException
from flow_info.runs_cache import RunsCache
from flow_info.run_logs_cache import RunLogsCache
from flow_info.data_manager import DataManager


log = logging.getLogger(__name__)


class FlowsCache:
    def __init__(self, name: str, date: datetime.datetime = None, cfg: str = None):
        self.name = name

        if self.name is None:
            raise ConfigException("Flows Cache cannot be created with name=None")

        # self.date = date or datetime.datetime.now()
        self.date = datetime.datetime(year=2025, month=5, day=1)
        self.cfg_filename = (
            cfg or pathlib.Path(__file__).parent.parent / "beamlines.cfg"
        )
        log.info(f"Using CFG filename: {self.cfg_filename}")
        self.config = configobj.ConfigObj(str(self.cfg_filename))

        if not self.config["beamlines"].get(self.name):
            err = f'"{self.name}" is not configured in {self.cfg_filename}. Please add the following entry under [ "beamlines" ]\n\n'
            err = f'{err}[\[ {self.name} ]]\n\tname = "{self.name}"\n\tclient_id = "<client_id>"\n'
            raise ConfigException(err)

        if not self.cfg.get("path"):
            self.cfg["path"] = pathlib.Path(__file__).parent.parent / "data"
        self.basepath = self.cfg["path"]
        self.basepath.mkdir(exist_ok=True)
        log.debug(f"Using data path: {self.basepath}")

        self.runs_cache = RunsCache(self.get_client_app(), self.config, self.name)
        self.run_logs_cache = RunLogsCache(
            self.get_client_app(), self.config, self.name
        )
        self.data_manager = DataManager(self.config, name)

    @property
    def flows_list_filename(self):
        return f"{self.cfg['name']}-{self.date.year}-{self.date.month}-flows.json"

    @functools.cache
    def _load_data(self, filename: str):
        path = pathlib.Path(self.basepath) / filename
        log.debug(f"Loading: {path}")
        if not path.exists():
            return []
        with open(path) as f:
            data = f.read()
            if data:
                return json.loads(data)
            return None

    def _save_data(self, filename: str, data):
        path = pathlib.Path(self.basepath) / filename
        log.debug(f"Saving: {path}")
        with open(path, "w") as f:
            f.write(json.dumps(data, indent=2))
        self._load_data.cache_clear()

    @property
    def runs_list_filename(self):
        return f"{self.cfg['name']}-{self.date.year}-{self.date.month}-runs.json"

    @property
    def run_logs_filename(self):
        """
        TODO: This doesn't really make sense, and doesn't account for all logs.
        """
        return f"{self.cfg['name']}-{self.date.year}-{self.date.month}-run-logs.json"

    @property
    def cfg(self):
        return self.config["beamlines"][self.name]

    def get_client_app(self):
        key = f"{self.name.upper()}_CLIENT_SECRET"
        secret = os.getenv(key)
        if not secret:
            err = f'export {key}="<secret>"'
            raise ConfigException(f"Please set {key} to fetch data for client")

        app = globus_sdk.ClientApp(
            app_name=f"FlowInfo-{self.cfg.get('name', self.name)}",
            client_id=self.cfg["client_id"],
            client_secret=secret,
        )
        return app

    def get_flows_client(self):
        return globus_sdk.FlowsClient(app=self.get_client_app())

    @property
    def flows(self):
        data = self._load_data(self.flows_list_filename)
        if data:
            return data["flows"]
        return []

    def get_runs(self, year_month: str):
        yield from self.runs_cache.get_runs(year_month)

    def sizeof(self, filename: str) -> int:
        if os.path.exists(self.basepath / filename):
            return os.stat(self.basepath / filename).st_size
        return 0

    def get_flow(self, flow_id: str):
        log.debug(f"Looking up flow {flow_id}")
        for flow in self.flows:
            if flow["flow_id"] == flow_id:
                return flow

    def get_available_caches(self):
        return sorted(self.data_manager.get_available_runs())

    def update_runs(self):
        yield from self.runs_cache.update_runs()

    def update_flows(self, limit=0):
        flows_client = self.get_flows_client()
        flows = list(
            flows_client.paginated.list_flows(
                # query_params={"orderby": ("created_at DESC",), "limit": 1},
            ).items()
        )

        flows = {"flows": flows}
        log.info(f'Fetched {len(flows["flows"])} Flows from service.')
        self._save_data(self.flows_list_filename, flows)

    def update_run_logs(self, callback):
        caches = self.get_available_caches()
        for cache in caches:
            log.debug(f"Fetching {cache} runs...")
            runs = self.runs_cache.get_runs([cache])
            yield cache, len(caches)
            # ids = [r["run_id"] for r in runs]
            self.run_logs_cache.update_run_logs(runs, cache, callback)

    def get_last_run(self):
        flows_client = self.get_flows_client()

        runs = flows_client.list_runs(
            query_params={"orderby": ("start_time DESC",), "limit": 1}
        )
        return runs.data["runs"][0]

    def summary(self):

        runs = list(self.get_runs(["2025-05"]))
        flows = self.flows

        last_run = self.get_last_run()
        last_cached_run = runs[-1] if runs else {}

        lrt = last_run.get("start_time", datetime.datetime.now().isoformat())
        last_run_time = datetime.datetime.fromisoformat(lrt)

        log.debug(f"Last Cached run: {last_cached_run}")
        log.debug(f"Comparing: {lrt}, {last_cached_run.get('start_time')}")

        return {
            "name": self.cfg["name"],
            "last_run": last_run_time,
            "runs": len(runs),
            "flows": len(flows),
            "runs_size": self.sizeof(self.runs_list_filename),
            "flows_size": self.sizeof(self.flows_list_filename),
            "run_logs_size": self.sizeof(self.run_logs_filename),
            "cache_up_to_date": lrt == last_cached_run.get("start_time"),
        }
