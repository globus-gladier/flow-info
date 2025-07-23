import globus_sdk
import json
import logging
import re
import pathlib
import datetime
from flow_info.data_manager import DataManager

log = logging.getLogger(__name__)
MAX_SEARCH_LIMIT = 10000


class RunsCache:
    def __init__(self, app: globus_sdk.GlobusApp, config, name):
        self.app = app
        self.data_manager = DataManager(config, name)
        # self.config = config
        # self.name = name

    def get_runs(self, year_months: list = None):
        for year_month in year_months or self.data_manager.get_available_year_months():
            yield from self.data_manager.load_runs(year_month).get("runs", [])

    def get_date_interval(self, date_str):
        """
        Parse a date interval by date_str.

        date_str: 2025-07 -- "month"
        date_str: 2025-07-01 -- "day"
        date_str: 2025-06-08 18:00:00 -- "hour"
        """
        if date_str is None:
            return None
        date_bits = date_str.split("-")
        if len(date_bits) == 2:
            # 2025-06
            return "month"
        if len(date_bits) == 3:
            if len(date_str.split(" ")) == 2:
                # 2025-06-08 18:00:00
                return "hour"
            # 2025-06-08
            return "day"
        raise ValueError(f"Unsupported date str: {date_str}")

    def get_date_filters(self, date_str):
        date_type = self.get_date_interval(date_str)
        if date_type is None:
            return []
        elif date_type == "month":
            y, m = date_str.split("-")
            y, m = int(y), int(m)
            val = {
                "gte": f"{y}-{str(m).zfill(2)}",
                "lt": f"{y}-{str(m % 12 + 1).zfill(2)}",
            }
        elif date_type == "day":
            lower = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            upper = datetime.datetime.strptime(
                date_str, "%Y-%m-%d"
            ) + datetime.timedelta(days=1)
            val = {"gte": lower.date().isoformat(), "lt": upper.date().isoformat()}
        elif date_type == "hour":
            lower = datetime.datetime.fromisoformat(date_str)
            upper = datetime.datetime.fromisoformat(date_str) + datetime.timedelta(
                hours=1
            )
            val = {"gte": lower.isoformat(), "lt": upper.isoformat()}
        filters = [
            {
                "type": "range",
                "field_name": "start_time",
                "values": [val],
            }
        ]
        # log.debug(f"Filter: {filters[0]['values'][0]}")
        return filters

    def get_buckets_by_date(self, date_str=None):
        """
        Fetch facet buckets from Globus Search and return a list of the buckets by date.
        Supports three different intervals of date string, from month to day.

        We should never need to facet differently, since runs are destroyed from Globus
        Search in 90 days. And it's implausable that there would be anyone crazy enough
        to do more than 10,000 runs in an hour.

        Returns a list of Globus Search faceted buckets with a date range set on the values.
        For example:

        date_str: None       -- Facet interval: month, Filter: None
        date_str: 2025-07    -- Facet interval: day,   Filter: 2025-07--2025-08
        date_str: 2025-07-01 -- Facet interval: hour,  Filter: 2025-07-01--2025-02-01

        :param date_str: A date string. Supported "2025-07", "2025-07-01" or isoformat with time.
        """
        sc = globus_sdk.SearchClient(app=self.app)
        date_interval = self.get_date_interval(date_str)
        if date_interval is None:
            di = "month"
        elif date_interval == "month":
            di = "day"
        elif date_interval == "day":
            di = "hour"
        else:
            raise NotImplementedError("Cannot filter results based on {date_str}")

        request = {
            "q": "*",
            "limit": "0",
            "@version": "query#1.0.0",
            "facets": [
                {
                    "name": "Started",
                    "type": "date_histogram",
                    "field_name": "start_time",
                    "date_interval": di,
                }
            ],
            "filters": self.get_date_filters(date_str),
        }
        r = sc.post_search("2a318659-a547-4b48-a0fc-e0c19081a960", request)
        f = self.get_date_filters(date_str)
        if f:
            fs = f"{f[0]['values'][0]['gte']} -- {f[0]['values'][0]['lt']}"
        else:
            fs = ""
        log.debug(
            f"{request['facets'][0]['date_interval']} with filters {fs} Totaling {r.data['total']} results."
        )
        return r["facet_results"][0]["buckets"]

    def get_incomplete_buckets(self):
        incomplete_buckets = []
        for bucket in self.get_buckets_by_date():
            saved_runs = self.load_runs(bucket["value"])
            log.info(
                f"{self.get_filename(bucket['value'])}: {len(saved_runs['runs'])}/{bucket['count']}."
            )
            if len(saved_runs["runs"]) < bucket["count"]:
                if bucket["count"] > MAX_SEARCH_LIMIT:
                    log.warning(
                        f"LARGE BUCKET DETECTED ({bucket['count']} for {bucket['value']}), FETCHING..."
                    )
                    for bucket_by_day in self.get_buckets_by_date(bucket["value"]):
                        if bucket_by_day["count"] > MAX_SEARCH_LIMIT:
                            log.warning(
                                f"ANOTHER LARGE BUCKET DETECTED ({bucket_by_day['count']} for {bucket_by_day['value']}), FETCHING..."
                            )
                            for bucket_by_hour in self.get_buckets_by_date(
                                bucket_by_day["value"]
                            ):
                                incomplete_buckets.append(
                                    {
                                        "value": bucket_by_hour["value"],
                                        "current": 0,
                                        "count": bucket_by_hour["count"],
                                    }
                                )
                        else:
                            incomplete_buckets.append(
                                {
                                    "value": bucket_by_day["value"],
                                    "current": 0,
                                    "count": bucket_by_day["count"],
                                }
                            )
                else:
                    incomplete_buckets.append(
                        {
                            "value": bucket["value"],
                            "current": len(saved_runs),
                            "count": bucket["count"],
                        }
                    )
        return incomplete_buckets

    def update_runs(self):
        sc = globus_sdk.SearchClient(app=self.app)
        buckets = self.get_incomplete_buckets()
        total = sum([b["count"] for b in buckets])
        current = sum([b["current"] for b in buckets])
        yield current, total

        # Fetch a new batch of runs from Globus Search.
        current_run_batch = None
        run_data = {"runs": []}
        for bucket in buckets:
            log.debug(f"Fetching {bucket['value']}: count {bucket['count']}")
            request = {
                "q": "*",
                "limit": MAX_SEARCH_LIMIT,
                "@version": "query#1.0.0",
                "sort": [{"field_name": "start_time", "order": "asc"}],
                "filters": self.get_date_filters(bucket["value"]),
            }

            # Check the buckets for a date change. If the month has clicked over, we want
            # to save the current batch of runs and start on the next one.
            date_val = bucket["value"].split(" ")[0].split("-")
            year_month_batch_key = f"{date_val[0]}-{date_val[1]}"
            if current_run_batch is None:
                current_run_batch = year_month_batch_key
            elif current_run_batch != year_month_batch_key:
                log.info(
                    f"Checkpoint Reached! Saving {len(run_data['runs'])} for {current_run_batch}..."
                )
                self.save_data(current_run_batch, run_data)
                run_data = {"runs": []}
                current_run_batch = year_month_batch_key

            # Add the latest batch of runs from lobus Search
            for result in sc.paginated.post_search(
                "2a318659-a547-4b48-a0fc-e0c19081a960", request
            ):
                runs = [e["entries"][0]["content"] for e in result["gmeta"]]
                run_data["runs"] += runs
                current += len(runs)
                yield current, total

        if current_run_batch is None:
            log.debug("No runs to fetch, all done!")
        else:
            self.save_data(current_run_batch, run_data)
            log.info(f"Completed {current}/{total}. All info saved.")
