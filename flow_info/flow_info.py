import logging
import pandas as pd

from flows_cache import FlowsCache


log = logging.getLogger(__name__)


class FlowInfo:
    """A class to inspect and describe Globus Flow runs.
    """

    def __init__(self, name="xpcs"):
        self.cache = FlowsCache(name)
        self.flow_logs = {}

    def load(self, limit=20):
        """Load a flow's executions

        Args:
            limit (int, optional): The number of flow actions to load. Defaults 100.
        """
        self.flow_logs, self.action_logs = self._extract_times(self.cache.runs[0:limit])
        log.info(f"Loaded {limit} runs")
        return self.flow_logs

    def _get_step_types(self, flow_id):
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
        action_logs = {}
        for flow_run in flow_runs:
            if flow_run["status"] != "SUCCEEDED":
                log.debug(f"Skipping run {flow_run['run_id']} due to status: {flow_run['status']}")
                continue

            step_types = self._get_step_types(flow_run['flow_id'])

            flow_res = {}
            flow_res['action_id'] = flow_run['action_id']
            flow_res['start'] = flow_run['start_time']
            flow_res['end'] = flow_run['completion_time']

            log.debug(f"Fetching run action logs for {flow_run['action_id']}")
            flow_logs = self.cache.get_run_logs(flow_run['action_id'])
            action_logs[flow_res['action_id']] = flow_logs['entries'][-1]
            
            # Get step timing info. this gives a _runtime field for each step
            flow_steps, flow_order = self._extract_step_times(flow_logs)
            self.flow_order = flow_order
            flow_res.update(flow_steps)
            
            # Pull out bytes transferred from any Transfer steps
            bytes_transferred = self._extract_bytes_transferred(flow_logs)
            flow_res.update(bytes_transferred)

            # Get the list of funcx task uuids to later track function execution time
            funcx_task_ids = self._extract_funcx_info(flow_logs, step_types)
            flow_res['funcx_task_ids'] = funcx_task_ids

            flowdf = pd.DataFrame([flow_res])
            # Convert timing strings
            convert_columns = list(flowdf.columns)[1:]  # Not inlcude action id
            for c in convert_columns:
                if 'start' in c or 'end' in c:
                    flowdf[c] = (pd.to_datetime(flowdf[c]).dt.tz_localize(None) - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
            
            # Get a list of all the compute
            compute_steps = []
            for step, ap in step_types.items():
                if ap == "https://compute.actions.globus.org":
                    compute_steps.append(step)
            total_compute_time = 0

            # Compute runtime fields and add together compute total time
            flowdf['flow_runtime'] = flowdf['end'] - flowdf['start']
            for step in flow_order:
                flowdf[f'{step}_runtime'] = flowdf[f'{step}_end'] - flowdf[f'{step}_start']
                if step in compute_steps:
                    total_compute_time += flowdf[f'{step}_runtime']
            flowdf['total_funcx_time'] = total_compute_time

            all_res = pd.concat([all_res, flowdf], ignore_index=True)
        if len(all_res) > 0:
            all_res = all_res.sort_values(by=['start'])
            all_res = all_res.reset_index(drop=True)
        return all_res, action_logs


    def _extract_bytes_transferred(self, flow_logs):
        """Extract the bytes moved by Transfer steps

        Args:
            flow_logs (dict): A log of the flow's steps

        Returns:
            dict: A dict of the bytes moved for each step
        """
        flow_log = flow_logs['entries'][-1]

        transfer_usage = {}
        total_bytes_transferred = 0
        if not flow_log['details'].get('output') or not flow_log['details']['output'].get('details'):
            log.error("Flow encountered with no bytes transferred!")
            transfer_usage['total_bytes_transferred'] = total_bytes_transferred
            return transfer_usage
        for k, v in flow_log['details']['output'].items():
            if 'details' in v:
                if 'bytes_transferred' in v['details']:
                    transfer_usage[f"{k}_bytes_transferred"] = v['details']['bytes_transferred']
                    total_bytes_transferred += v['details']['bytes_transferred']
        
        transfer_usage['total_bytes_transferred'] = total_bytes_transferred
        return transfer_usage

    def _extract_funcx_info(self, flow_logs: dict, step_types: dict):
        """Extract funcx task information. Work out function uuids and the runtime of all tasks

        Args:
            flow_logs (dict): A log of the flow's steps

        Returns:
            dict: A dict of funcx runtimes and a list of tasks
        """

        fx_steps = []
        for step, ap in step_types.items():
            if 'funcx' in ap:
                fx_steps.append(step)

        fx_ids = []
        
        flow_log = flow_logs['entries'][-1]

        for k, v in flow_log['details']['output'].items():
            if k in fx_steps:
                fx_ids.append(v['action_id'])

        return fx_ids


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
