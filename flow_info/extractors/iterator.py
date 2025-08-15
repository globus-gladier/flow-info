import pandas as pd
import logging

from .transfer import TransferExtractor

log = logging.getLogger(__name__)


class RunLogIterator:

    def __init__(self, cache):
        self.cache = cache
        self.extract_stats = {}
        self.extractors = [TransferExtractor()]

        self.extraction_results = []
        self.stats = {
            "skipped_missing_log": 0,
            "skipped_status": 0,
            "skipped_empty": 0,
            "successful": 0,
            "errors": 0,
        }

        log.debug(f"Loaded Extractors: {self.extractor_names}")

    @property
    def extractor_names(self):
        return [e.name for e in self.extractors]

    def run_extractors(self, year_months=None, test=False):
        available = self.cache.get_available_caches()
        year_months = year_months or available
        if not set(year_months).issubset(set(available)):
            raise ValueError(
                f"Not available: {set(available).difference(set(year_months))}"
            )

        # all_res = pd.DataFrame()
        log.debug("EXTRACTING")
        flows = {f["id"]: f for f in self.cache.get_flows()}
        for cache in self.cache.get_available_caches():
            log.debug(f"Cache: {cache}")
            runs = list(self.cache.get_runs([cache]))
            logs = list(self.cache.get_run_logs(runs, [cache]))
            log.debug(f"{len(runs)} Runs, {len(list(logs))} Logs.")
            # print(logs[0])
            for run, run_log_item in zip(runs, list(logs)):
                run_log_id, run_log = run_log_item
                assert run["run_id"] == run_log_id, "Log ID does not match run!"
                if run_log is None:
                    self.stats["skipped_missing_log"] += 1
                    continue

                log.debug(f"Extracting {run['run_id']}")

                for extractor in self.extractors:
                    extraction = {}
                    log.debug(f"Extracting {run['run_id']} using {extractor.name}")
                    if (
                        extractor.filters.get("statuses")
                        and run["status"] not in extractor.filters["statuses"]
                    ):
                        self.stats["skipped_status"] += 1
                        continue

                    try:
                        ex = extractor.extract(flows.get(run["flow_id"]), run, run_log)
                    except Exception:
                        self.stats["skipped_error"] += 1
                        continue
                    if ex is None:
                        self.stats["skipped_empty"] += 1
                    else:
                        self.stats["successful"] += 1
                        self.extraction_results.append(extraction)

                    if test is True and self.stats["successful"]:
                        log.debug(f"Stats: {self.stats}")
                        return
            # log.debug(f"stopping after one")
            # return

    def get_stats(self):
        return self.extraction_results

    def get_all_results(self):
        return self.extraction_results

    def get_results(self, extractor: str):
        if extractor not in self.extractor_names:
            raise ValueError(
                f"No known extractor, {extractor}, only {self.extractor_names}"
            )
        return [r.get(extractor) for r in self.extraction_results]
