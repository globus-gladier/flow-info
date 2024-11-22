import os
import logging
import logging.config
import typing as t

import typer
import humanize
import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import track
from rich.table import Column
from rich.progress import Progress, BarColumn, TextColumn
from flow_info import plots, flow_info, flows_cache

log = logging.getLogger(__name__)
app = typer.Typer(no_args_is_help=True)
console = Console()


def fmt_time(seconds_passed: int) -> str:
    """Format time in a nice human-readable format."""
    return humanize.naturaldelta(datetime.timedelta(seconds=seconds_passed))


TYPER_OP_LIMIT = typer.Option(default=0, help="Limit the amount of runs to examine.")


@app.command()
def summary(name: str = "xpcs"):
    fc = flows_cache.FlowsCache(name)
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

    with Progress() as progress:

        flows_task = progress.add_task("[red]Downloading Flows...")
        runs_task = progress.add_task("[green]Downloading Runs...")
        run_logs_task = progress.add_task("[cyan]Downloading Run Logs...")

        fc.update_flows()
        progress.update(flows_task, advance=100.0)
        if fc.summary()["cache_up_to_date"] is False:
            for runs_fetched in fc.update_runs():
                progress.update(
                    runs_task,
                    advance=1,
                    description=f"[green]Downloading Runs...{runs_fetched}",
                )
        progress.update(runs_task, advance=100)
        fc._load_data.cache_clear()
        total_runs = len(fc.runs)
        for idx, run in enumerate(fc.runs):
            progress.update(
                run_logs_task,
                advance=1,
                total=total_runs,
                description=f"[cyan]Downloading Run Logs...({idx}/{total_runs})",
            )
            fc.get_run_logs(run["run_id"])


@app.command()
def runtimes(name: str = "xpcs", limit: int = TYPER_OP_LIMIT):
    fi = flow_info.FlowInfo(name)
    flow_logs = fi.load(limit=limit)

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
def transfer_usage(
    name: str = "xpcs",
    limit: int = TYPER_OP_LIMIT,
    filter_transfer_states: t.List[str] = None,
):
    fi = flow_info.FlowInfo(name, transfer_states=filter_transfer_states or list())
    list(track(fi.load(limit=limit)))
    flow_logs = fi.flow_stats

    t_states = [
        k.replace("_bytes_transferred", "")
        for k in flow_logs.keys()
        if "_bytes_transferred" in k and k != "total_bytes_transferred"
    ]

    table = Table(
        "Name",
        "Sum",
        "Mean",
        "Files Transferred",
        "Mean Files Transferred",
        "Files Skipped",
        "Mean Files Skipped",
    )
    for state in t_states:
        btrans = f"{state}_bytes_transferred"
        ftrans = f"{state}_files_transferred"
        fskip = f"{state}_files_skipped"
        table.add_row(
            state,
            humanize.naturalsize(flow_logs[btrans].sum()),
            humanize.naturalsize(flow_logs[btrans].mean()),
            str(flow_logs[ftrans].sum()),
            str(flow_logs[ftrans].mean()),
            str(flow_logs[fskip].sum()),
            str(flow_logs[fskip].mean()),
        )
    table.add_row(
        "Total",
        humanize.naturalsize(flow_logs["total_bytes_transferred"].sum()),
        humanize.naturalsize(flow_logs["total_bytes_transferred"].mean()),
        str(flow_logs["total_files_transferred"].sum()),
        str(flow_logs["total_files_transferred"].mean()),
        str(flow_logs["total_files_skipped"].sum()),
        str(flow_logs["total_files_skipped"].mean()),
    )
    console.print(table)


@app.command()
def compute_usage(
    name: str = "xpcs",
    limit: int = TYPER_OP_LIMIT,
    filter_transfer_states: t.List[str] = None,
):
    fi = flow_info.FlowInfo(name, transfer_states=filter_transfer_states or list())
    flow_logs = fi.load(limit=limit)

    t_states = [
        k.replace("_compute_time", "")
        for k in flow_logs.keys()
        if "_compute_time" in k and k != "total_compute_time"
    ]

    table = Table("Name", "Total Compute Time", "Average Compute Time")
    for state in t_states:
        btime = f"{state}_compute_time"
        table.add_row(
            state,
            fmt_time(flow_logs[btime].sum()),
            fmt_time(flow_logs[btime].mean()),
        )
    table.add_row(
        "Total",
        fmt_time(flow_logs["total_compute_time"].sum()),
        fmt_time(flow_logs["total_compute_time"].mean()),
    )
    console.print(table)


@app.command()
def histogram(name: str = "xpcs"):
    fi = flow_info.FlowInfo(name)
    fi.load()
    plots.plot_histogram(fi.flow_logs)


@app.command()
def gantt(name: str = "xpcs"):
    fi = flow_info.FlowInfo(name)
    fi.load()
    plots.plot_gantt(fi.flow_logs, fi.flow_order)


@app.command()
def update_logs(name: str = "xpcs"):
    fc = flows_cache.FlowsCache(name)
    for run in fc.runs:
        console.log(f"Updating run logs for run id {run_id}")
        fc.get_run_logs(run["run_id"])


@app.callback()
def main(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.WARNING
    # Log stuff in here
    logging.config.dictConfig(
        {
            "version": 1,
            "formatters": {
                "basic": {
                    "format": "[%(levelname)s] " "%(name)s::%(funcName)s() %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "rich.logging.RichHandler",
                    "level": level,
                    "console": console,
                }
            },
            "loggers": {
                "flow_info": {"level": "DEBUG", "handlers": ["console"]},
            },
        }
    )


if __name__ == "__main__":
    app()
