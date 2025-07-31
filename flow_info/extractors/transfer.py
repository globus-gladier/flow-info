from .extractor import BaseExtractor


class TransferExtractor(BaseExtractor):

    filters = {
        # "action_state": "ActionCompleted",
        # "action_providers": [
        #     # Old Transfer AP -- Remove after Feb 2025
        #     "https://actions.automate.globus.org/transfer/transfer/",
        #     # New Transfer AP
        #     "https://transfer.actions.globus.org/transfer/"
        # ],
        "statuses": ["SUCCEEDED"],
        # "missing": ["flow", "run_log"],
        "tags": [],
    }

    def extract(self, flow, run, run_logs):
        """Extract the bytes moved by Transfer steps

        Args:
            flow_logs (dict): A log of the flow's steps

        Returns:
            dict: A dict of the bytes moved for each step
        """
        data = {
            "total_bytes_transferred": 0,
            "total_files_transferred": 0,
            "total_files_skipped": 0,
            "state_names": [],
        }
        for lg in run_logs["entries"]:

            state_name = lg["details"].get("state_name")
            print(state_name)
            if not state_name or state_name != "ActionCompleted":
                continue

            action_logs = lg["details"]["output"]
            data["state_names"].append(state_name)
            data[f"{state_name}_bytes_transferred"] = action_logs[state_name][
                "details"
            ]["bytes_transferred"]
            data[f"{state_name}_files_transferred"] = action_logs[state_name][
                "details"
            ]["files_transferred"]
            data[f"{state_name}_files_skipped"] = action_logs[state_name]["details"][
                "files_skipped"
            ]

            data["total_bytes_transferred"] += action_logs[state_name]["details"][
                "bytes_transferred"
            ]
            data["total_files_transferred"] += action_logs[state_name]["details"][
                "files_transferred"
            ]
            data["total_files_skipped"] += action_logs[state_name]["details"][
                "files_skipped"
            ]
        if not data["state_names"]:
            return
        print(f"Run Status: {run['status']}")
        return data
