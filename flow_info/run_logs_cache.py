import globus_sdk
import json
import logging
import re
import pathlib
import datetime
import asyncio
from zoneinfo import ZoneInfo
from flow_info.data_manager import DataManager

log = logging.getLogger(__name__)


class RunLogsCache:

    BUCKET_SIZE = 2500

    def __init__(self, app: globus_sdk.GlobusApp, config, name, workers=3):
        self.app = app
        self.data_manager = DataManager(config, name)
        self.workers = workers

    def get_run_logs(self, year_month: str, runs):
        run_list = sorted(runs, key=lambda x: x["start_time"])
        buckets = list(self.partition_buckets(run_list))
        for bucket_num, bucket in buckets:
            run_logs = self.data_manager.load_run_logs(year_month, bucket_num) or {"logs": {}}
            for run in bucket:
                yield run_logs["logs"].get(run["run_id"])

    def update_run_logs(self, runs, year_month, callback=None):
        run_list = sorted(runs, key=lambda x: x["start_time"])
        buckets = list(self.partition_buckets(run_list))
        for bucket_num, bucket in buckets:
            run_logs = self.data_manager.load_run_logs(year_month, bucket_num) or {
                "logs": {}
            }
            log.debug(
                f"Loaded {len(run_logs['logs'])} run logs for {year_month} bucket {bucket_num} runs {len(bucket)}"
            )
            exc = None
            try:
                asyncio.run(self._update_run_logs_loop(bucket, run_logs, bucket_num, len(run_list), callback))
                yield bucket_num, len(buckets)
            except KeyboardInterrupt as e:
                log.warning("Interrupt Received! Saving and exciting...")
                exc = e
            finally:
                self.data_manager.save_run_logs(year_month, run_logs, bucket_num)
                if exc:
                    raise KeyboardInterrupt()

    async def _update_single_run_log(
        self,
        worker_name: str,
        flows_client: globus_sdk.FlowsClient,
        queue,
        run_logs: dict,
    ):
        log.debug(f"Worker {worker_name} started.")
        while True:
            try:
                run_id = await queue.get()
                log.debug(f"Fetching new run {run_id}")
                run_log = await asyncio.to_thread(
                    flows_client.get_run_logs, run_id, limit=100
                )
                run_logs["logs"][run_id] = run_log.data

                # Notify the queue that the "work item" has been processed.
                queue.task_done()
                log.debug("Success!")
            except Exception as e:
                log.exception(e)

    def is_expired(self, run):
        start_time = datetime.datetime.fromisoformat(run["start_time"])
        expired = datetime.timedelta(days=90)
        return bool(datetime.datetime.now(ZoneInfo("UTC")) - start_time >= expired)

    def partition_buckets(self, runs):
        return enumerate(
            [
                runs[i : i + self.BUCKET_SIZE]
                for i in range(0, len(runs), self.BUCKET_SIZE)
            ]
        )

    async def _update_run_logs_loop(self, runs: list, run_logs: dict, bucket_number: int, total_runs, callback: callable):
        # Prep the queue
        fetch_queue = asyncio.Queue()
        rejected = []
        accounted = []
        for run in runs:
            if self.is_expired(run):
                rejected.append(run)
            elif run["run_id"] in run_logs.get("logs", {}):
                accounted.append(run)
            else:
                fetch_queue.put_nowait(run["run_id"])

        bucket_total = sum((len(rejected), len(accounted), fetch_queue.qsize()))
        log.debug(
            f"{len(rejected)}/{bucket_total} logs expired, {len(accounted)}/{bucket_total} accounted for, and {fetch_queue.qsize()}/{bucket_total} need to be fetched."
        )
        flows_client = globus_sdk.FlowsClient(app=self.app)
        tasks = []
        for i in range(self.workers):
            task = asyncio.create_task(
                self._update_single_run_log(
                    f"worker-{i}", flows_client, fetch_queue, run_logs
                )
            )
            tasks.append(task)

        while not fetch_queue.empty():
            # Total run logs fetched in previous runs
            total_previous = bucket_number * self.BUCKET_SIZE
            # The amount finished during this fetch.
            currently_finished = bucket_total - fetch_queue.qsize()
            callback(total_previous + currently_finished, total_runs)
            await asyncio.sleep(1)
        log.debug(f"Finishing remaining tasks...")

        await fetch_queue.join()

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
