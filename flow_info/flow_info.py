import logging
import datetime
import pandas as pd
import typing as t

from flow_info.flows_cache import FlowsCache


log = logging.getLogger(__name__)


class FlowInfo:
    """A class to inspect and describe Globus Flow runs.
    """

    transfer_ap_urls = [
        # Old Transfer AP -- Remove after Feb 2025
        "https://actions.automate.globus.org/transfer/transfer/",
        # New Transfer AP
        "https://transfer.actions.globus.org/transfer/"
    ]
    compute_ap_urls = [
        "https://compute.actions.globus.org",
    ]

    def __init__(self, name="xpcs"):
        self.cache = FlowsCache(name)
        self.missing_run_logs = 0
        self.flow_stats = {}

    def load(self, limit=20, step_times_compute_only=False):
        """Load a flow's executions

        Args:
            limit (int, optional): The number of flow actions to load. Defaults 100.
        """
        runs = self.cache.runs[0:limit] if limit else self.cache.runs
        log.debug(f"Fetching metadata for {len(runs)} runs...")
        return self._extract_times(runs, step_times_compute_only)

    def get_missing_run_logs(self) -> t.Tuple[int, int]:
        return self.missing_run_logs, len(self.cache.get_run_logs())

    def get_flow_stats(self) -> dict:
        if self.flow_stats.empty:
            raise ValueError("Must call FlowStats.load() before stats can be collected.")
        return self.flow_stats

    def get_step_types(self, flow_id):
        """Get the type associated with each step.

        Args:
            flow_id (str): The id of the flow
        
        Returns:
            Dict: A dict of step name and action url
        """
        flow_dfn = None
        for flow in self.cache.flows:
            if flow_id == flow["id"]:
                flow_dfn = flow
                break
        if not flow_dfn:
            raise ValueError(f"Could not find flow {flow_id}")

        steps = {}
        for x in flow_dfn['definition']['States']:
            if flow_dfn['definition']['States'][x]["Type"] == "Action":
                steps[x] = flow_dfn['definition']['States'][x]['ActionUrl']
        return steps

    def filter_log_entries(self, run_log: dict, filter_state_names: t.List[str], filter_codes: t.List[str] = ["ActionCompleted"]) -> t.List[dict]:
        """
        :param run_log: Full dict containing all run log info
        :param filter_state_names: Names that will match this state.
        :param filter_code: Status code to filter log entries by. Common ones are ActionStarted, ActionCompleted
        """
        return [
            e for e in run_log['entries'] if
            e["code"] in filter_codes and
            (len(filter_state_names) == 0 or e['details']['state_name'] in filter_state_names)
        ]

    def filter_ap_states(self, flow_id: str, action_provider_urls: t.List[str]) -> t.Set[str]:
        return {state_name for state_name, url in self.get_step_types(flow_id).items() if url in action_provider_urls}

    def filter_ap_states_transfer(self, flow_id: str):
        return self.filter_ap_states(flow_id, self.transfer_ap_urls)

    def filter_ap_states_compute(self, flow_id: str):
        return self.filter_ap_states(flow_id, self.compute_ap_urls)


    def _extract_times(self, flow_runs, step_times_compute_only: bool = False):
        """Extract the timings from the flow logs and create a dataframe

        Args:
            flow_runs (dict): A dict of flow runs

        Returns:
            DataFrame: A dataframe of the flow execution steps
            Dict: A dict of step name to bytes transferred
        """
        self.missing_run_logs = 0
        all_res = pd.DataFrame()
        for flow_run in flow_runs:
            if flow_run["status"] != "SUCCEEDED":
                log.debug(f"Skipping run {flow_run['run_id']} due to status: {flow_run['status']}")
                continue

            log.debug(f"Fetching run action logs for {flow_run['run_id']}")
            flow_logs = self.cache.get_run_logs(flow_run['run_id'])
            if not flow_logs:
                self.missing_run_logs += 1
                continue

            # Collect info about the flow run
            flow_res = {"start": flow_run["start_time"]}
            flow_res.update(self.extract_bytes_transferred(flow_run["flow_id"], flow_logs))

            # Filter state names by compute if required
            filter_names = self.filter_ap_states_compute(flow_run["flow_id"]) if step_times_compute_only else []
            flow_res.update(self.extract_step_times(flow_logs, filter_state_names=filter_names))

            # Combine dataframes
            flowdf = pd.DataFrame([flow_res])
            all_res = pd.concat([all_res, flowdf], ignore_index=True)

            # Yield the current progress. The number of iterated runs is significant, so this is nice to track progress
            yield
        if len(all_res) > 0:
            all_res = all_res.sort_values(by=['start'])
            all_res = all_res.reset_index(drop=True)
        log.debug("Done!")
        self.flow_stats = all_res


    def extract_step_times(self, flow_logs, filter_state_names: t.List[str] = []):
        step_times = {
            "total_step_time": 0,
        }
        flogs = self.filter_log_entries(flow_logs, filter_state_names, ["ActionStarted", "ActionCompleted"])
        log.debug(f"Filtering compute log states based on: {filter_state_names}, Matched entries: {len(flogs)}")
        stats = {}
        for lg in flogs:
            state_name = lg["details"].get("state_name")
            if lg["code"] == "ActionStarted":
                stats[state_name] = {"start": lg["time"]}
            elif lg["code"] == "ActionCompleted":
                stats[state_name]["end"] = lg["time"]
        step_times = {
            f"{name}_step_time": (datetime.datetime.fromisoformat(vals["end"]) - datetime.datetime.fromisoformat(vals["start"])).total_seconds() for name, vals in stats.items()
        }
        step_times["total_step_time"] = sum(step_times.values())
        return step_times


    def extract_bytes_transferred(self, flow_id: str, flow_logs: dict):
        """Extract the bytes moved by Transfer steps

        Args:
            flow_logs (dict): A log of the flow's steps

        Returns:
            dict: A dict of the bytes moved for each step
        """
        transfer_usage = {
            "total_bytes_transferred": 0,
            "total_files_transferred": 0,
            "total_files_skipped": 0
        }
        filter_state_names = self.filter_ap_states_transfer(flow_id)
        flogs = self.filter_log_entries(flow_logs, filter_state_names, ["ActionCompleted"])
        log.debug(f"Filtering log states based on: {filter_state_names}, Matched entries: {len(flogs)}")

        for lg in flogs:
            action_logs = lg["details"]["output"]
            state_name = lg["details"].get("state_name")
            if state_name is None:
                log.warning(f"No statename found in log entry! (filtering on {filter_state_names})")
                continue

            transfer_usage[f"{state_name}_bytes_transferred"] = action_logs[state_name]['details']['bytes_transferred']
            transfer_usage[f"{state_name}_files_transferred"] = action_logs[state_name]['details']['files_transferred']
            transfer_usage[f"{state_name}_files_skipped"] = action_logs[state_name]['details']['files_skipped']

            transfer_usage["total_bytes_transferred"] += action_logs[state_name]['details']['bytes_transferred']
            transfer_usage["total_files_transferred"] += action_logs[state_name]['details']['files_transferred']
            transfer_usage["total_files_skipped"] += action_logs[state_name]['details']['files_skipped']

        return transfer_usage


    def extract_dates(self):

        dates = pd.DataFrame()
        for flow_run in self.cache.runs:
            dt = datetime.datetime.fromisoformat(flow_run["start_time"])
            round_hour = dt.hour // 30
            run_info = pd.DataFrame([{
                "start_date": dt.date().isoformat(),
                "start_hour": dt.replace(second=0, microsecond=0, minute=0, hour=dt.hour + round_hour)
            }])
            dates = pd.concat([dates, run_info], ignore_index=True)

        # Value counts for start date
        value_counts = dates["start_date"].value_counts()
        dates = dates.assign(runs_per_day=[value_counts[sd] for sd in dates["start_date"]])

        # Value counts for start hour
        hour_counts = dates["start_hour"].value_counts()
        dates = dates.assign(runs_per_hour=[value_counts[sd.date().isoformat()] for sd in dates["start_hour"]])
        return dates


if __name__ == "__main__":
    fi = FlowInfo()
    fi.load(limit=10)

    fi.describe_runtimes()

    fi.describe_usage()
