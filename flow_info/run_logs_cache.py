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
    def __init__(self, app: globus_sdk.GlobusApp, config, name, workers=10):
        self.app = app
        self.data_manager = DataManager(config, name)
        self.workers = workers

    def get_run_logs(self, run_id: str):
        run_logs = self._load_data(self.run_logs_filename) or {"logs": {}}
        if run_id in run_logs["logs"]:
            return run_logs["logs"][run_id]

    def update_run_logs(self, runs, year_month, callback=None):

        run_logs = self.data_manager.load_run_logs(year_month) or {"logs": {}}
        log.debug(f"Loaded {len(run_logs['logs'])} run logs for {year_month}")
        exc = None
        try:
            asyncio.run(self._update_run_logs_loop(runs, run_logs, callback))
        except KeyboardInterrupt as e:
            log.warning("Interrupt Received! Saving and exciting...")
            exc = e
        finally:
            self.data_manager.save_run_logs(year_month, run_logs)
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

    async def is_expired(self, run):
        start_time = datetime.datetime.fromisoformat(run["start_time"])
        expired = datetime.timedelta(days=90)
        return bool(datetime.datetime.now(ZoneInfo("UTC")) - start_time >= expired)

    async def _update_run_logs_loop(self, runs: list, run_logs: dict, callback=None):
        # Prep the queue
        fetch_queue = asyncio.Queue()
        rejected = []
        accounted = []
        for run in runs:
            if await self.is_expired(run):
                rejected.append(run)
            elif run["run_id"] in run_logs.get("logs", {}):
                accounted.append(run)
            else:
                fetch_queue.put_nowait(run["run_id"])

        total = sum((len(rejected), len(accounted), fetch_queue.qsize()))
        log.debug(
            f"{len(rejected)}/{total} logs expired, {len(accounted)}/{total} accounted for, and {fetch_queue.qsize()}/{total} need to be fetched."
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
            if callback:
                callback(total - fetch_queue.qsize(), total)
            else:
                log.debug(f"Working on queue ({total - fetch_queue.qsize()}/{total})")
            await asyncio.sleep(1)
        log.debug(f"Finishing remaining tasks...")

        await fetch_queue.join()
        callback(100, 100)

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log.debug("Exciting...")
