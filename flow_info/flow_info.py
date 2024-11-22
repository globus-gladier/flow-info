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

    def __init__(self, name="xpcs", compute_states: t.List[str] = [], transfer_states: t.List[str] = []):
        self.compute_states = compute_states
        self.transfer_states = transfer_states

        self.cache = FlowsCache(name)
        self.flow_logs = {}

    def load(self, limit=20):
        """Load a flow's executions

        Args:
            limit (int, optional): The number of flow actions to load. Defaults 100.
        """
        runs = self.cache.runs[0:limit] if limit else self.cache.runs
        log.debug(f"Fetching metadata for {len(runs)} runs...")
        return self._extract_times(runs)

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


    def _extract_times(self, flow_runs):
        """Extract the timings from the flow logs and create a dataframe

        Args:
            flow_id (str): The flow uuid
            flow_scope (str): The flow scope
            flow_runs (dict): A dict of flow runs

        Returns:
            DataFrame: A dataframe of the flow execution steps
            Dict: A dict of step name to bytes transferred
        """
        all_res = pd.DataFrame()
        for flow_run in flow_runs:
            if flow_run["status"] != "SUCCEEDED":
                log.debug(f"Skipping run {flow_run['run_id']} due to status: {flow_run['status']}")
                continue

            step_types = self.get_step_types(flow_run['flow_id'])

            flow_res = {}
            flow_res['start'] = flow_run['start_time']
            flow_res['end'] = flow_run['completion_time']

            log.debug(f"Fetching run action logs for {flow_run['run_id']}")
            flow_logs = self.cache.get_run_logs(flow_run['run_id'])
            
            # Get step timing info. this gives a _runtime field for each step
            flow_steps, flow_order = self._extract_step_times(flow_logs)
            self.flow_order = flow_order
            flow_res.update(flow_steps)
            
            # Pull out bytes transferred from any Transfer steps
            filtered_transfer = self.filter_ap_states_transfer(flow_run["flow_id"])
            if self.transfer_states:
                filtered_transfer = filtered_transfer.intersection(set(self.transfer_states))
                log.debug(f"Filtering on states: {filtered_transfer}")
            bytes_transferred = self.extract_bytes_transferred(flow_logs, filter_state_names=filtered_transfer, filter_codes=["ActionCompleted"])
            flow_res.update(bytes_transferred)

            # Pull out bytes transferred from any Compute steps
            filtered_compute = self.filter_ap_states_compute(flow_run["flow_id"])
            if self.compute_states:
                filtered_compute = filtered_compute.intersection(set(self.compute_states))
                log.debug(f"Filtering on states: {filtered_compute}")
            compute_stats = self.extract_compute_time(flow_logs, filter_state_names=filtered_compute)
            flow_res.update(compute_stats)

            flowdf = pd.DataFrame([flow_res])
            all_res = pd.concat([all_res, flowdf], ignore_index=True)

            yield
        if len(all_res) > 0:
            all_res = all_res.sort_values(by=['start'])
            all_res = all_res.reset_index(drop=True)
        log.debug("Done!")
        self.flow_stats = all_res


    def extract_compute_time(self, flow_logs, filter_state_names: t.List[str] = []):
        compute_times = {
            "total_compute_time": 0,
        }
        stats = {}
        if not filter_state_names:
            return compute_time
        flogs = self.filter_log_entries(flow_logs, filter_state_names, ["ActionStarted", "ActionCompleted"])
        log.debug(f"Filtering compute log states based on: {filter_state_names}, Matched entries: {len(flogs)}")

        for lg in flogs:
            state_name = lg["details"].get("state_name")
            if lg["code"] == "ActionStarted":
                stats[state_name] = {"start": lg["time"]}
            elif lg["code"] == "ActionCompleted":
                stats[state_name]["end"] = lg["time"]
                # stats[state_name]["action_id"] = lg["details"]["output"][state_name]["action_id"]
        compute_times = {
            f"{name}_compute_time": (datetime.datetime.fromisoformat(vals["end"]) - datetime.datetime.fromisoformat(vals["start"])).total_seconds() for name, vals in stats.items()
        }
        compute_times["total_compute_time"] = sum(compute_times.values())
        return compute_times


    def extract_bytes_transferred(self, flow_logs, filter_state_names: t.List[str] = [], filter_codes: str = ["ActionCompleted"]):
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
        if not filter_state_names:
            return transfer_usage
        flogs = self.filter_log_entries(flow_logs, filter_state_names, filter_codes)
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

    def _extract_step_times(self, flow_logs):
        """Extract start and stop times from a flow's logs

        Args:
            flow_logs (dict): A log of the flow's steps

        Returns:
            dict: A dict of the start and end times of each step
        """
        
        mylogs = flow_logs['entries']
        steps = []
        res = {}

        for x in range(len(mylogs)):            
            if 'Action' not in mylogs[x]['code']:
                continue
            if 'ActionStarted' in mylogs[x]['code']:
                name = mylogs[x]['details']['state_name']
                res[f'{name}_start'] = mylogs[x]['time']
                steps.append(name)
            if 'ActionCompleted' in mylogs[x]['code']:
                name = mylogs[x]['details']['state_name']
                res[f'{name}_end'] = mylogs[x]['time']
        
        return res, steps


if __name__ == "__main__":
    fi = FlowInfo()
    fi.load(limit=10)

    fi.describe_runtimes()

    fi.describe_usage()
