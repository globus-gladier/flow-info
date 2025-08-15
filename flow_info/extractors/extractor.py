class BaseExtractor:

    def __init__(self, name=None):
        self.name = name or self.__class__.__name__

    def get_step_types(self, flow_id):
        """Get the type associated with each step.

        Args:
            flow_id (str): The id of the flow

        Returns:
            Dict: A dict of step name and action url
        """
        flow_dfn = self.flows.get(flow_id)
        if not flow_dfn:
            raise ValueError(f"Could not find flow {flow_id}")

        steps = {}
        for x in flow_dfn["definition"]["States"]:
            if flow_dfn["definition"]["States"][x]["Type"] == "Action":
                steps[x] = flow_dfn["definition"]["States"][x]["ActionUrl"]
        return steps

    def filter_log_entries(
        self,
        run_log: dict,
        filter_state_names: t.List[str],
        filter_codes: t.List[str] = ["ActionCompleted"],
    ) -> t.List[dict]:
        """
        :param run_log: Full dict containing all run log info
        :param filter_state_names: Names that will match this state.
        :param filter_code: Status code to filter log entries by. Common ones are ActionStarted, ActionCompleted
        """
        return [
            e
            for e in run_log["entries"]
            if e["code"] in filter_codes
            and (
                len(filter_state_names) == 0
                or e["details"]["state_name"] in filter_state_names
            )
        ]

    def filter_ap_states(
        self, flow_id: str, action_provider_urls: t.List[str]
    ) -> t.Set[str]:
        return {
            state_name
            for state_name, url in self.get_step_types(flow_id).items()
            if url in action_provider_urls
        }

    def filter_ap_states_transfer(self, flow_id: str):
        return self.filter_ap_states(flow_id, self.transfer_ap_urls)

    def filter_ap_states_compute(self, flow_id: str):
        return self.filter_ap_states(flow_id, self.compute_ap_urls)
