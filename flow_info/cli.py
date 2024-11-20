import os
import logging

import typer
import humanize
import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table
import plots
from flows_cache import FlowsCache
from flow_info import FlowInfo

log = logging.getLogger(__name__)
app = typer.Typer(no_args_is_help=True)
console = Console()


def fmt_time(seconds_passed: int) -> str:
    """Format time in a nice human-readable format."""
    return humanize.naturaldelta(datetime.timedelta(seconds=seconds_passed))


@app.command()
def summary(name: str = "xpcs"):
    fc = FlowsCache(name)
    items = ["name", "runs", "flows", "last_run", "run_logs_size", "cache_up_to_date"]
    table = Table(*items)
    summary = fc.summary()

    summary["last_run"] = summary["last_run"].strftime("%A %d. %B %Y")
    summary["runs"] = (
        f"{summary['runs']} ({humanize.naturalsize(summary['runs_size'])})"
    )
    summary["flows"] = (
        f"{summary['flows']} ({humanize.naturalsize(summary['flows_size'])})"
    )
    summary["run_logs_size"] = (
        f"Runs log Cache: {humanize.naturalsize(summary['run_logs_size'])}"
    )
    summary["cache_up_to_date"] = str(summary["cache_up_to_date"])

    table.add_row(*[summary[name] for name in items])
    console.print(table)


@app.command()
def update(name: str = "xpcs"):
    fc = FlowsCache(name)
    console.print("Updating Flows...")
    fc.update_flows()
    console.print("Updating Runs...")
    fc.update_runs()


@app.command()
def usage(name: str = "xpcs"):
    fi = FlowInfo(name)
    flow_logs = fi.load()

    table = Table("Name", "Mean", "Median", "Min", "Max")
    for step in fi.flow_order:
        c = f"{step}_runtime"
        table.add_row(
            c,
            fmt_time(flow_logs[c].mean()),
            fmt_time(flow_logs[c].median()),
            fmt_time(flow_logs[c].min()),
            fmt_time(flow_logs[c].max()),
        )
    table.add_row(
        "Total",
        fmt_time(flow_logs["flow_runtime"].mean()),
        fmt_time(flow_logs["flow_runtime"].median()),
        fmt_time(flow_logs["flow_runtime"].min()),
        fmt_time(flow_logs["flow_runtime"].max()),
    )
    console.print(table)


@app.command()
def runtimes(name: str = "xpcs"):
    fi = FlowInfo(name)
    flow_logs = fi.load()

    table = Table(
        "Total Transferred",
        "Mean Transferred",
        "Total Compute Time",
        "Mean compute Time",
    )
    table.add_row(
        humanize.naturalsize(flow_logs["total_bytes_transferred"].sum()),
        humanize.naturalsize(flow_logs["total_bytes_transferred"].mean()),
        fmt_time(flow_logs["total_funcx_time"].sum()),
        fmt_time(flow_logs["total_funcx_time"].mean()),
    )
    console.print(table)


@app.command()
def histogram(name: str = "xpcs"):
    fi = FlowInfo(name)
    fi.load()
    plots.plot_histogram(fi.flow_logs)


@app.command()
def gantt(name: str = "xpcs"):
    fi = FlowInfo(name)
    fi.load()
    plots.plot_gantt(fi.flow_logs, fi.flow_order)


@app.command()
def update_logs(name: str = "xpcs"):
    fc = FlowsCache(name)
    for run in fc.runs:
        console.log(f"Updating run logs for run id {run_id}")
        fc.get_run_logs(run["run_id"])


if __name__ == "__main__":
    # Log stuff in here
    logger = logging.getLogger("root")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    # SDK Logs are so darn chatty...
    sdk = logging.getLogger("globus_sdk")
    sdk.setLevel(logging.WARNING)

    app()
