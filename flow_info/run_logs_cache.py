import globus_sdk
import logging
import datetime
import asyncio
from zoneinfo import ZoneInfo
from flow_info.data_manager import DataManager
from flow_info.database import UNFINISHED_STATUSES

log = logging.getLogger(__name__)


class RunLogsCache:

    # Run logs to fetch per batch, used for progress reporting.
    BATCH_SIZE = 500

    def __init__(self, app: globus_sdk.GlobusApp, config, data_manager=None):
        self.app = app
        self.flows_client = globus_sdk.FlowsClient(app=self.app)
        self.data_manager = data_manager or DataManager(config)

    def get_run_logs(self, runs, year_month: str):
        """
        Yield (run_id, run_log) for each of the month's runs, oldest first. run_log is
        None if no log is stored.
        """
        yield from self.data_manager.get_run_logs(year_month, runs)

    def update_run_logs(self, year_month, callback, workers):
        """Fetch and save logs for the month's runs that don't have one yet.

        Each log is saved as soon as it's fetched, so memory use stays at about one log
        per worker, and an interrupted update only loses the logs in flight.

        Yields (batches_done, num_batches) after each batch.
        """
        missing = self.data_manager.runs_missing_logs(year_month)
        live = [(run_id, status) for run_id, start_time, status in missing
                if not self.is_expired(start_time)]
        # A log fetched before its run finishes would be saved incomplete and never
        # fetched again, so unfinished runs wait until a runs update shows them done.
        pending = [run_id for run_id, status in live if status not in UNFINISHED_STATUSES]
        log.debug(
            f"{year_month}: {len(missing) - len(live)} missing logs have expired, "
            f"{len(live) - len(pending)} runs are unfinished, "
            f"{len(pending)} need to be fetched."
        )
        batches = [
            pending[i : i + self.BATCH_SIZE] for i in range(0, len(pending), self.BATCH_SIZE)
        ]
        if not batches:
            yield 1, 1
            return
        for batch_num, batch in enumerate(batches):
            asyncio.run(
                self._update_run_logs_loop(
                    batch,
                    year_month,
                    batch_num * self.BATCH_SIZE,
                    len(pending),
                    callback,
                    workers,
                )
            )
            yield batch_num + 1, len(batches)

    async def _update_single_run_log(
        self,
        worker_name: str,
        flows_client: globus_sdk.FlowsClient,
        queue,
        year_month: str,
    ):
        log.debug(f"Worker {worker_name} started.")
        while True:
            run_id = await queue.get()
            try:
                run_log = await asyncio.to_thread(
                    flows_client.get_run_logs, run_id, limit=100
                )
                # Runs on the event loop thread, so only one thread uses the database.
                self.data_manager.save_run_log(year_month, run_id, run_log.data)
            except Exception as e:
                # The log isn't saved, so the next update will retry it.
                log.exception(e)
            finally:
                # Always notify the queue that the "work item" has been processed,
                # otherwise queue.join() would wait forever on a failed fetch.
                queue.task_done()

    def is_expired(self, start_time: str):
        start_time = datetime.datetime.fromisoformat(start_time)
        expired = datetime.timedelta(days=90)
        return bool(datetime.datetime.now(ZoneInfo("UTC")) - start_time >= expired)

    async def _update_run_logs_loop(
        self,
        run_ids: list,
        year_month: str,
        done_before: int,
        total: int,
        callback: callable,
        workers: int,
    ):
        """Fetch and save logs for run_ids with a pool of workers."""
        fetch_queue = asyncio.Queue()
        for run_id in run_ids:
            fetch_queue.put_nowait(run_id)

        tasks = [
            asyncio.create_task(
                self._update_single_run_log(
                    f"worker-{i}", self.flows_client, fetch_queue, year_month
                )
            )
            for i in range(workers)
        ]

        while not fetch_queue.empty():
            callback(done_before + len(run_ids) - fetch_queue.qsize(), total)
            await asyncio.sleep(1)
        log.debug(f"Finishing remaining tasks...")

        await fetch_queue.join()
        callback(done_before + len(run_ids), total)

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
