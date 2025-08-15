import os
import logging
import logging.config
import typing as t
import pathlib

import typer
import configobj
import humanize
import datetime
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import track
from rich.table import Column
from rich.progress import Progress, BarColumn, TextColumn
from flow_info import plots, flow_info, flows_cache, exc

log = logging.getLogger(__name__)
app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)

console = Console()


def fmt_time(seconds_passed: int) -> str:
    """Format time in a nice human-readable format."""
    return humanize.naturaldelta(datetime.timedelta(seconds=seconds_passed))


TYPER_OP_LIMIT = typer.Option(default=0, help="Limit the amount of runs to examine.")


def get_flows_cache(config) -> flow_info.FlowInfo:
    """Return a FlowInfo object for the given name."""
    # if date is not None:
    #     date = datetime.datetime.strptime(date, "%Y-%m-%d")
    # else:
    #     date = datetime.datetime.now()
    return flows_cache.FlowsCache(config)


def get_config():
    # self.date = date or datetime.datetime.now()
    date = datetime.datetime(year=2025, month=5, day=1)
    cfg_filename = pathlib.Path(__file__).parent.parent / "beamlines.cfg"
    log.info(f"Using CFG filename: {cfg_filename}")
    config = configobj.ConfigObj(str(cfg_filename))

    name = config["beamlines"]["current_app"]

    if not config["beamlines"].get(name):
        err = f'"{name}" is not configured in {cfg_filename}. Please add the following entry under [ "beamlines" ]\n\n'
        err = f'{err}[\[ {name} ]]\n\tname = "{name}"\n\tclient_id = "<client_id>"\n'
        raise ConfigException(err)

    if not config["beamlines"].get("path"):
        config["beamlines"]["path"] = pathlib.Path(__file__).parent.parent / "data"
    basepath = config["beamlines"]["path"]
    basepath.mkdir(exist_ok=True)
    log.debug(f"Using data path: {basepath}")
    return config


@app.command()
def summary(refresh_cache_info: bool = False):
    items = ["name", "date", "flows", "runs", "run_logs", "missing_logs"]
    table = Table(*items)
    config = get_config()

    fc = get_flows_cache(config)
    if refresh_cache_info:
        fc.refresh_cache_info()
    for month in fc.summary():
        table.add_row(
            month["name"],
            month["year_month"],
            str(month["flows"]),
            f"{month['runs']} ({humanize.naturalsize(month['runs_file_size'])})",
            f"{month['run_logs']} ({humanize.naturalsize(month['run_logs_file_size'])})",
            f"{month['missing_logs']}",
        )
    console.print(table)


@app.command()
def update(
    gui: bool = True,
    flows: bool = False,
    runs: bool = False,
    logs: bool = False,
    workers: int = 3,
):
    fc = get_flows_cache(get_config())
    fc.login()

    flag = flows or runs or logs
    if flag:
        flows, runs, logs = flag and flows, flag and runs, flag and logs
    else:
        flows = runs = logs = True

    if gui is False:
        if flows:
            console.print("Updating Flows...")
            fc.update_flows()
        if runs:
            console.print("Updating Runs...")
            for current, total in fc.update_runs():
                console.print(f"Fetching: ({current}/{total})")
        if logs:
            console.print("Updating Run Logs...")
            for (
                cache,
                caches,
                batch,
                total_batches,
                log_cache_progress,
                total,
            ) in fc.update_run_logs(
                lambda x, n: console.print(f"Updating runs {x}/{n}"), workers=workers
            ):
                console.print(
                    f"Updating Cache {cache} ({(caches.index(cache) + 1)}/{len(caches)}), Batch ({batch}/{total_batches}) Total Progress {log_cache_progress:.2f}%"
                )
        return

    with Progress() as progress:

        flows_task = progress.add_task("[red]Downloading Flows...")
        runs_task = progress.add_task("[green]Downloading Runs...")
        run_logs_cache = progress.add_task("[yellow]Updating Cache...")
        run_logs_task = progress.add_task("[cyan]Downloading Run Logs...")

        if flows:
            fc.update_flows()
            progress.update(flows_task, advance=100.0)
        if runs:
            for completed, total in fc.update_runs():
                progress.update(
                    runs_task,
                    completed=completed,
                    total=total,
                    description=f"[green]Downloading Runs...({completed}/{total})",
                )
            # Set runs task to finished
            progress.update(runs_task, completed=1, total=1)
        if logs:
            for (
                cache,
                caches,
                batch,
                total_batches,
                log_cache_progress,
                total,
            ) in fc.update_run_logs(
                lambda x, n: progress.update(
                    run_logs_task,
                    completed=x,
                    total=n,
                    description=f"[cyan]Downloading Run Logs...({x}/{n})",
                ),
                workers,
            ):
                progress.update(
                    run_logs_cache,
                    completed=log_cache_progress,
                    total=total,
                    description=f"[yellow]Updating Cache...({cache} [{(caches.index(cache) + 1)}/{len(caches)}] -- Batch {batch}/{total_batches})",
                )


@app.command()
def plot_runs_over_time():
    config = get_config()
    fc = get_flows_cache(config)
    datetimes = [
        datetime.datetime.fromisoformat(r["start_time"]) for r in fc.get_runs()
    ]
    filename = plots.plot_runs_over_time(
        datetimes, f'{config["beamlines"]["current_app"]}'
    )
    console.print(f"Plotted {len(datetimes)} runs and saved to {filename}")


@app.command()
def transfer_usage(
    limit: int = TYPER_OP_LIMIT,
    filter_transfer_states: t.List[str] = None,
):
    fi = flow_info.FlowInfo(get_flows_cache(get_config()))
    # Track progress through iterations of logs
    list(track(fi.load(limit=limit)))
    flow_logs = fi.get_flow_stats()

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
def runtimes(
    limit: int = TYPER_OP_LIMIT,
    compute_only: bool = False,
):
    """
    todo: Collect number of runs present in each step
    """
    fi = flow_info.FlowInfo(get_flows_cache(get_config()))
    list(track(fi.load(limit=limit, step_times_compute_only=compute_only)))
    flow_logs = fi.get_flow_stats()

    t_states = [
        k.replace("_step_time", "")
        for k in flow_logs.keys()
        if "_step_time" in k and k != "total_step_time"
    ]

    table = Table(
        "Name", "Total Compute Time", "Average Compute Time", "Min", "Max", "Corr Time"
    )
    for state in t_states:
        btime = f"{state}_step_time"
        table.add_row(
            state,
            fmt_time(flow_logs[btime].sum()),
            f"{flow_logs[btime].mean():.2f} seconds",
            f"{flow_logs[btime].min():.2f} seconds",
            f"{flow_logs[btime].max():.2f} seconds",
            # f"{flow_logs['corr_execution_time'].max():.2f} seconds",
        )
    table.add_row(
        "Total",
        fmt_time(flow_logs["total_step_time"].sum()),
        f"{flow_logs['total_step_time'].mean():.2f} seconds",
        fmt_time(flow_logs["total_step_time"].min()),
        fmt_time(flow_logs["total_step_time"].max()),
    )
    console.print(f"Collected metadata for {len(flow_logs)} runs.")
    console.print(table)


@app.command()
def plot_step_times():
    config = get_config()
    fi = flow_info.FlowInfo(get_flows_cache(config))
    amount = len(list(track(fi.load())))

    app = config["beamlines"]["current_app"]
    filename = plots.plot_step_times(fi.get_flow_stats(), name=app)
    console.print(f"Generated gantt with {amount} logs and saved to {filename}.")


@app.command()
def plot_gantt():
    config = get_config()
    fi = flow_info.FlowInfo(get_flows_cache(config))
    amount = len(list(track(fi.load())))

    app = config["beamlines"]["current_app"]
    order = config["beamlines"][app]["flow_order"]
    filename = plots.plot_gantt(fi.get_flow_stats(), order, name=app)
    console.print(f"Generated gantt with {amount} logs and saved to {filename}.")


@app.callback()
def main(ctx: typer.Context, verbose: bool = False):

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


def main_cli():
    try:
        app()
    except exc.ConfigException as e:
        console.log(f"Config Error: {str(e)}")


if __name__ == "__main__":
    main_cli()
