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
    def __init__(self, name: str, config: configobj.ConfigObj):

        self.name = name
        self.config = config
        if self.config is None:
            raise ValueError("Config can't be none!")
        self.runs_cache = RunsCache(self.get_client_app(), self.config, self.name)
        self.run_logs_cache = RunLogsCache(
            self.get_client_app(), self.config, self.name
        )
        self.data_manager = DataManager(self.config, name)

    def get_client_app(self):
        key = f"{self.name.upper()}_CLIENT_SECRET"
        secret = os.getenv(key)
        if not secret:
            err = f'export {key}="<secret>"'
            raise ConfigException(f"Please set {key} to fetch data for client")

        app = globus_sdk.ClientApp(
            app_name=f"FlowInfo-{self.config['beamlines'][self.name]}",
            client_id=self.config["beamlines"][self.name]["client_id"],
            client_secret=secret,
        )
        return app

    def get_flows_client(self):
        return globus_sdk.FlowsClient(app=self.get_client_app())

    def get_flows(self, year_month):
        return self.data_manager.load_flows(year_month)

    def get_runs(self, year_month: str):
        return self.runs_cache.get_runs(year_month)

    def get_run_logs(self, year_month: str):
        return self.run_logs_cache.get_run_logs(year_month)

    def get_flow(self, flow_id: str):
        log.debug(f"Looking up flow {flow_id}")
        now = datetime.datetime.now()
        for flow in self.get_flows(f"{now.year}-{now.month}"):
            if flow["flow_id"] == flow_id:
                return flow

    def get_available_caches(self):
        return sorted(self.data_manager.get_available_runs())

    def update_runs(self):
        yield from self.runs_cache.update_runs()

    def update_flows(self, limit=0):
        flows_client = self.get_flows_client()
        flows = list(flows_client.paginated.list_flows().items())

        flows = {"flows": flows}
        log.info(f'Fetched {len(flows["flows"])} Flows from service.')
        now = datetime.datetime.now()
        self.data_manager.save_flows(f"{now.year}-{now.month}", flows)

    def update_run_logs(self, callback):
        caches = self.get_available_caches()
        for cache in caches:
            log.debug(f"Fetching {cache} runs...")
            runs = self.runs_cache.get_runs([cache])
            yield cache, len(caches)
            self.run_logs_cache.update_run_logs(runs, cache, callback)

    def summary(self, year_months: list = None) -> list:

        year_months = year_months or self.get_available_caches()
        summaries = []
        for year_month in year_months:
            summaries.append(
                {
                    "name": self.name,
                    "year_month": year_month,
                    "flows": len(self.get_flows(year_month)),
                    "runs": len(list(self.get_runs([year_month]))),
                    "run_logs": len(list(self.get_run_logs(year_month)["logs"])),
                    "runs_file_size": self.data_manager.get_runs_file_size(year_month),
                    "run_logs_file_size": self.data_manager.get_run_logs_file_size(
                        year_month
                    ),
                }
            )
        return summaries
