import datetime
import json
import os
import pathlib
import configobj
import logging
import functools
import globus_sdk


log = logging.getLogger(__name__)


class FlowsCache:

    def __init__(self, name: str, date: datetime.datetime = None, cfg: str = None):
        self.name = name

        if self.name is None:
            raise ValueError("Flows Cache cannot be created with name=None")

        self.date = date or datetime.datetime.now()
        self.cfg_filename = (
            cfg or pathlib.Path(__file__).parent.parent / "beamlines.cfg"
        )
        log.debug(f"Using CFG filename: {self.cfg_filename}")
        self.config = configobj.ConfigObj(str(self.cfg_filename))

        self.basepath = (
            self.cfg.get("path") or pathlib.Path(__file__).parent.parent / "data"
        )
        self.basepath.mkdir(exist_ok=True)
        log.debug(f"Using data path: {self.basepath}")

    @property
    def flows_list_filename(self):
        return f"{self.cfg['name']}-flows-{self.date.year}-{self.date.month}.json"

    @property
    def runs_list_filename(self):
        return f"{self.cfg['name']}-runs-{self.date.year}-{self.date.month}.json"

    @property
    def run_logs_filename(self):
        return f"{self.cfg['name']}-run-logs-{self.date.year}-{self.date.month}.json"

    @property
    def cfg(self):
        return self.config["beamlines"][self.name]

    def get_flows_client(self):
        key = f"{self.name.upper()}_CLIENT_SECRET"
        secret = os.getenv(key)
        if not secret:
            raise ValueError("Please set {key} to fetch data for client")

        app = globus_sdk.ClientApp(
            app_name=f"FlowInfo-{self.cfg.get('name', self.name)}",
            client_id=self.cfg["client_id"],
            client_secret=secret,
        )
        return globus_sdk.FlowsClient(app=app)

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
    def flows(self):
        data = self._load_data(self.flows_list_filename)
        if data:
            return data["flows"]
        return []

    @property
    def runs(self):
        data = self._load_data(self.runs_list_filename)
        if data:
            return data["runs"]
        return []

    def sizeof(self, filename: str) -> int:
        if os.path.exists(self.basepath / filename):
            return os.stat(self.basepath / filename).st_size
        return 0

    def get_run_logs(self, run_id: str):
        run_logs = self._load_data(self.run_logs_filename) or {"logs": {}}

        if run_id in run_logs["logs"]:
            log.debug(f"Run logs cache hit for run id {run_id}")
            return run_logs["logs"][run_id]

        # Fetch and save the log
        flows_client = self.get_flows_client()
        log.debug(f"Run logs cache MISS for run id {run_id}")
        run_log = flows_client.get_run_logs(run_id, limit=100).data
        run_logs["logs"][run_id] = run_log
        self._save_data(self.run_logs_filename, run_logs)

        return run_log

    def get_flow(self, flow_id: str):
        log.debug(f"Looking up flow {flow_id}")
        for flow in self.flows:
            if flow["flow_id"] == flow_id:
                return flow

    def update_runs(self, limit=0):
        flows_client = self.get_flows_client()

        runs = list()
        runs_collected = 0
        for idx, run in enumerate(
            flows_client.paginated.list_runs(
                query_params={"orderby": ("completion_time DESC",)}
            )
        ):
            runs += run.data["runs"]
            if limit and runs_collected > limit:
                break
            runs_collected += len(run.data["runs"])
            log.debug(f"Fetched {runs_collected} runs...")
            yield runs_collected

        # Save only run data, not other junk returned by flows
        run_data = {"runs": runs}
        log.info(f'Fetched {len(run_data["runs"])} runs from service.')
        self._save_data(self.runs_list_filename, run_data)

    def update_flows(self, limit=0):
        flows_client = self.get_flows_client()
        flows = list(
            flows_client.paginated.list_flows(
                query_params={"orderby": ("created_at DESC",), "limit": 1},
            ).items()
        )

        flows = {"flows": flows}
        log.info(f'Fetched {len(flows["flows"])} Flows from service.')
        self._save_data(self.flows_list_filename, flows)

    def _update_single_run_log(
        self, flows_client: globus_sdk.FlowsClient, run_id: str, run_logs: dict
    ):
        run_log = flows_client.get_run_logs(run_id, limit=100).data
        run_logs["logs"][run_id] = run_log

    def update_run_logs(self):
        self._load_data.cache_clear()
        run_logs = self._load_data(self.run_logs_filename) or {"logs": {}}
        need_to_fetch = [
            r["run_id"] for r in self.runs if r["run_id"] not in run_logs["logs"]
        ]

        flows_client = self.get_flows_client()
        try:
            for idx, run_id in enumerate(need_to_fetch):
                self._update_single_run_log(flows_client, run_id, run_logs)
                log.debug(f"Fetched run {idx}/{len(need_to_fetch)}: {run_id}")
                yield idx, len(need_to_fetch)
        except KeyboardInterrupt:
            log.critical("Received interrupt! Saving data to disk...")
            self._save_data(self.run_logs_filename, run_logs)
            raise
        log.info("Update successful!")
        self._save_data(self.run_logs_filename, run_logs)

    def get_last_cached_run(self, runs):
        if not runs:
            return dict()
        return sorted(runs, key=lambda x: x["completion_time"], reverse=True)[0]

    def get_last_run(self):
        flows_client = self.get_flows_client()

        runs = flows_client.list_runs(
            query_params={"orderby": ("completion_time DESC",), "limit": 1}
        )
        return runs.data["runs"][0]

    def summary(self):

        runs = self.runs
        flows = self.flows

        last_run = self.get_last_run()
        last_cached_run = self.get_last_cached_run(runs)

        lrt = last_run.get("completion_time", datetime.datetime.now().isoformat())
        last_run_time = datetime.datetime.fromisoformat(lrt)

        log.debug(f"Comparing: {lrt}, {last_cached_run.get('completion_time')}")

        return {
            "name": self.cfg["name"],
            "last_run": last_run_time,
            "runs": len(runs),
            "flows": len(flows),
            "runs_size": self.sizeof(self.runs_list_filename),
            "flows_size": self.sizeof(self.flows_list_filename),
            "run_logs_size": self.sizeof(self.run_logs_filename),
            "cache_up_to_date": lrt == last_cached_run.get("completion_time"),
        }
