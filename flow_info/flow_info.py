import pandas as pd

from globus_automate_client import create_flows_client

class FlowInfo:
    """A class to inspect and describe Globus Flow runs.
    """

    def __init__(self):
        self.fc = create_flows_client()
        self.flow_id = None
        self.flow_scope = None
        self.flow_runs = []
        self.flow_logs = None

    def load(self, flow_id, flow_scope, limit=100):
        """Load a flow's executions

        Args:
            flow_id (str): The uuid of the flow
            flow_scope (str): The globus scope of the flow
            limit (int, optional): The number of flow actions to load. Defaults 100.
        """
        self.flow_id = flow_id
        self.flow_scope = flow_scope
        self.flow_runs = []
        self.flow_order = []

        self.flow_runs = self.get_flow_runs(self.flow_id, limit)
        print(f"Found {len(self.flow_runs)} flow runs.")

        self.flow_logs = self._extract_times(flow_id, flow_scope, self.flow_runs)
        print(f"Loaded {len(self.flow_runs)} runs.")

    def get_flow_runs(self, flow_id, limit):
        """Get the runs of the flow using pagination.

        Args:
            flow_id (str): The flow id to use
            limit (int): Max flows to return
        """
        res_runs = []
        all_runs = []

        runs = self.fc.list_flow_runs(flow_id=flow_id, role="monitor_by")
        all_runs = all_runs + runs['actions']
        # Iterate through pages to collect more
        while runs['has_next_page']:
            if len(all_runs) >= limit:
                break
            print('Getting more runs')
            runs = self.fc.list_flow_runs(flow_id=flow_id, marker=runs['marker'])
            all_runs = all_runs + runs['actions']
        
        # Skip flows that didn't succeed
        for fr in all_runs:
            if fr['status'] == "SUCCEEDED":
                res_runs.append(fr)

        # Cut down to limit size
        res_runs = res_runs[:limit]
        return res_runs

    def describe_runtimes(self):
        """Print out summary stats for each step
        """
        if self.flow_logs is None:
            print("No flows data loaded.")
            return

        print(f"Flow:\t mean {int(self.flow_logs['flow_runtime'].mean())}s, "\
              f"min {int(self.flow_logs['flow_runtime'].min())}s, "\
              f"max {int(self.flow_logs['flow_runtime'].max())}s")

        for step in self.flow_order:
            c = f"{step}_runtime"
            print(f"{step}:\t mean {int(self.flow_logs[c].mean())}s, "\
                  f"min {int(self.flow_logs[c].min())}s, "\
                  f"max {int(self.flow_logs[c].max())}s")

    def _extract_times(self, flow_id, flow_scope, flow_runs):
        """Extract the timings from the flow logs and create a dataframe

        Args:
            flow_id (str]): The flow uuid
            flow_scope (str): The flow scope
            flow_runs (dict): A dict of flow runs

        Returns:
            DataFrame: A dataframe of the flow execution steps
        """
        all_res = pd.DataFrame()
        for flow_run in flow_runs:
            flow_res = {}
            flow_res['action_id'] = flow_run['action_id']
            flow_res['start'] = flow_run['start_time']
            flow_res['end'] = flow_run['completion_time']

            flow_logs = self.fc.flow_action_log(flow_id, flow_scope, flow_run['action_id'], limit=100)
            flow_steps, flow_order  = self._extract_step_times(flow_logs)
            self.flow_order = flow_order
            flow_res.update(flow_steps)
            flowdf = pd.DataFrame([flow_res])
            # Convert timing strings
            convert_columns = list(flowdf.columns)[1:]  # Not inlcude action id
            for c in convert_columns:
                flowdf[c] = (pd.to_datetime(flowdf[c]).dt.tz_localize(None) - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
                
            # Compute runtime fields
            flowdf['flow_runtime'] = flowdf['end'] - flowdf['start']
            for step in flow_order:
                flowdf[f'{step}_runtime'] = flowdf[f'{step}_end'] - flowdf[f'{step}_start']
                
            all_res = all_res.append(flowdf, ignore_index=True)
        if len(all_res) > 0:
            all_res = all_res.sort_values(by=['start'])
            all_res = all_res.reset_index(drop=True)
        return all_res

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

    def plot_histogram(self, include=None):
        """Create a histogram of the step runtimes
        
        Args:
            include (list, optional): The list of steps to plot, e.g. ['flow', '1', '2']. Defaults to None.
        """
        import matplotlib.pyplot as plt

        cols = []
        for c in self.flow_logs.keys():
            if '_runtime' in c:
                if not include or c.replace("_runtime", "") in include:
                    cols.append(c.replace("_runtime", ""))
        df = self.flow_logs[cols]

        f = plt.figure()
        df.plot.hist(bins=20, alpha=0.5, ax=f.gca())
        plt.legend(cols, loc='center left', bbox_to_anchor=(1.0, 0.5), fontsize=18)
        plt.ylabel('Frequency', fontsize=18, color='black')
        plt.xlabel('Time (s)', fontsize=18, color='black')
        plt.tick_params(axis='both', which='major', pad=-1, labelsize=18, labelcolor='black')


    def plot_gantt(self, limit=None, show_relative_time=True):
        """Plot a Gantt Chart of flow runs.

        Args:
            limit (str, optional): The number of most recent flows to plot
            show_relative_time (bool, optional): show relative time on x axis. Default: True
        """
        import numpy as np
        import matplotlib.pyplot as plt
        import seaborn as sns
        sns.set(style='white', palette="Set2", color_codes=False)
        sns.set_style("ticks")
        colors = sns.color_palette('Set2')
        
        tasks = self.flow_logs.copy()
        
        # Shrink the task list to the last n rows
        if limit:
            tasks = tasks.tail(limit)
            tasks = tasks.reset_index(drop=True)
        
        # Convert to relative time
        if show_relative_time:
            pd.options.display.float_format = '{:.5f}'.format
            convert_columns = list(tasks.columns)[1:]  # Not inlcude action id
            start = tasks['start'].min()
            for c in convert_columns:
                if "_runtime" not in c:
                    tasks[c] = tasks[c] - start

        # Plot from dataframe
        fig, gnt = plt.subplots(figsize=(12, 9))
        gnt.set_ylim(0, (len(tasks) + 1) * 10)
        gnt.grid(True) 
        gnt.set_yticks([(i+1) * 10 + 3 for i in range(len(tasks))]) 
        gnt.set_yticklabels(range(len(tasks)))

        for i, task in tasks.iterrows():
            flow_start = task['start']       
            for j, step in enumerate(self.flow_order):
                step_start, step_end = task[f'{step}_start'], task[f'{step}_end'] - task[f'{step}_start']
                if j == 0:
                    gnt.broken_barh([(flow_start, step_start-flow_start)], ((i+1)*10, 6), facecolor=colors[0], edgecolor='black')
                gnt.broken_barh([(step_start, step_end)], ((i+1)*10, 6), facecolor=colors[j+1], edgecolor='black')
            flow_end = task['end'] - task[f'{step}_end']
        gnt.legend(self.flow_order,
                    #'Flow finishing'],
                fontsize=25, loc='upper left')
        gnt.set_ylabel('Flow', fontsize=25, color='black')
        gnt.set_xlabel('Time (s)', fontsize=25, color='black')
        gnt.tick_params(axis='both', which='major', pad=-1, labelsize=25, labelcolor='black')


if __name__ == "__main__":

    flow_id = '13630c87-e8f9-41c7-b2f3-dc0a5c66d5c7'
    # flow_id = '3ba38fba-feee-42d1-8c99-8ce3b812fe49'
    flow_scope = f"https://auth.globus.org/scopes/{flow_id}/flow_{flow_id.replace('-','_')}_user"
    # flow_id = '3ba38fba-feee-42d1-8c99-8ce3b812fe49'
    # flow_scope = 'https://auth.globus.org/scopes/3ba38fba-feee-42d1-8c99-8ce3b812fe49/flow_3ba38fba_feee_42d1_8c99_8ce3b812fe49_user'
    
    fi = FlowInfo()
    fi.load(flow_id, flow_scope, limit=2)

    fi.describe_runtimes()
