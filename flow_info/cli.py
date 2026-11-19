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
from flow_info import plots, flow_info, flows_cache, exc, migration
from flow_info import data_manager as data_manager_module

log = logging.getLogger(__name__)
app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)

console = Console()


def fmt_time(seconds_passed: int) -> str:
    """Format time in a nice human-readable format."""
    return humanize.naturaldelta(datetime.timedelta(seconds=seconds_passed))


TYPER_OP_LIMIT = typer.Option(default=0, help="Limit the amount of runs to examine.")
TYPER_OP_RUN_RANGE = typer.Option(default="", help="The year-month range to examine, e.g. 2024-05 or 2024-05--2024-08")
TYPER_OP_ALL_RUNS = typer.Option(default=False, help="Load all runs regardless of date range.")

def parse_run_range(run_range: str) -> list[str]:
    """Parse a run_range option string into a list of year-month strings."""
    if len(run_range.split("--")) == 2:
        start, end = run_range.split("--")
        result = year_months_until_now(start)
        if end:
            result = result[: result.index(end) + 1]
        return result
    elif run_range:
        return [run_range]
    return []


def year_months_until_now(start_ym: str) -> list[str]:
    """
    Given a year-month string in 'YYYY-MM' format,
    return a list of year-months from that point up to the current month.
    """
    try:
        year, month = map(int, start_ym.split("-"))
        current = datetime.date(year, month, 1)
    except ValueError:
        raise ValueError("Input must be in 'YYYY-MM' format")

    today = datetime.date.today()
    end = datetime.date(today.year, today.month, 1)

    result = []

    while current <= end:
        result.append(f"{current.year:04d}-{current.month:02d}")

        # increment month
        if current.month == 12:
            current = datetime.date(current.year + 1, 1, 1)
        else:
            current = datetime.date(current.year, current.month + 1, 1)

    return result


def get_flows_cache(config) -> flow_info.FlowInfo:
    """Return a FlowInfo object for the given name."""
    # if date is not None:
    #     date = datetime.datetime.strptime(date, "%Y-%m-%d")
    # else:
    #     date = datetime.datetime.now()
    return flows_cache.FlowsCache(config)


def find_config_file() -> pathlib.Path:
    """Find beamlines.cfg: $FLOW_INFO_CONFIG, then the working directory, then the
    source checkout (which only exists for editable installs)."""
    candidates = []
    if os.getenv("FLOW_INFO_CONFIG"):
        candidates.append(pathlib.Path(os.environ["FLOW_INFO_CONFIG"]))
    candidates += [
        pathlib.Path.cwd() / "beamlines.cfg",
        pathlib.Path(__file__).parent.parent / "beamlines.cfg",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    tried = "\n".join(f"  {c}" for c in candidates)
    raise exc.ConfigException(
        f"Could not find beamlines.cfg. Run flow-info from the directory containing it, "
        f"or set FLOW_INFO_CONFIG to its path. Looked in:\n{tried}"
    )


def get_config():
    cfg_filename = find_config_file()
    log.info(f"Using CFG filename: {cfg_filename}")
    config = configobj.ConfigObj(str(cfg_filename))

    if "beamlines" not in config or "current_app" not in config["beamlines"]:
        raise exc.ConfigException(
            f'{cfg_filename} needs a [ "beamlines" ] section with current_app set'
        )
    name = config["beamlines"]["current_app"]

    if not config["beamlines"].get(name):
        err = f'"{name}" is not configured in {cfg_filename}. Please add the following entry under [ "beamlines" ]\n\n'
        err = f'{err}[[ {name} ]]\n\tname = "{name}"\n\tclient_id = "<client_id>"\n'
        raise exc.ConfigException(err)

    # A relative data_path is relative to the config file, so the same data folder is
    # used wherever flow-info is run from.
    basepath = cfg_filename.parent / config["beamlines"].get("data_path", "data/")
    config["beamlines"]["data_path"] = str(basepath)
    basepath.mkdir(exist_ok=True)
    log.debug(f"Using data path: {basepath}")
    return config


@app.command()
def summary(
    refresh_cache_info: bool = typer.Option(
        False,
        help="Recount runs and logs for months still stored as JSON. Migrated months "
        "are always counted from their database.",
    ),
):
    items = ["name", "date", "flows", "runs", "run_logs", "missing_logs", "size", "storage"]
    table = Table(*items)
    config = get_config()

    fc = get_flows_cache(config)
    if refresh_cache_info:
        fc.data_manager.legacy.refresh_counts()
    for month in fc.summary():
        table.add_row(
            month["name"],
            month["year_month"],
            str(month["flows"]),
            str(month["runs"]),
            str(month["run_logs"]),
            str(month["missing_logs"]),
            humanize.naturalsize(month["size"]),
            month["storage"],
        )
    console.print(table)


@app.command()
def migrate(
    months: t.List[str] = typer.Argument(None, help="Year-months to migrate, e.g. 2026-10 2026-11."),
    last: int = typer.Option(0, help="Migrate the N most recent months that are still JSON."),
    all_months: bool = typer.Option(False, "--all", help="Migrate every month that is still JSON."),
):
    """Convert months stored as JSON to SQLite. The JSON files are kept."""
    data_manager = data_manager_module.DataManager(get_config())
    legacy = [
        ym for ym in data_manager.get_available_runs() if data_manager.needs_migration(ym)
    ]
    if all_months:
        selected = legacy
    elif last:
        selected = legacy[-last:]
    elif months:
        unknown = [ym for ym in months if ym not in legacy]
        if unknown:
            console.print(f"Not JSON months that can be migrated: {' '.join(unknown)}")
            raise typer.Exit(code=1)
        selected = sorted(months)
    else:
        console.print("Pass months to migrate, --last N or --all.")
        console.print(f"Months still stored as JSON: {' '.join(legacy) or 'none'}")
        raise typer.Exit(code=1)

    if not selected:
        console.print("Nothing to migrate, every month is already SQLite.")
        return

    table = Table("Month", "Flows", "Runs", "Run logs", "JSON size", "SQLite size")
    for year_month in selected:
        console.print(f"Migrating {year_month}...")
        result = migration.migrate_month(data_manager, year_month, callback=console.print)
        table.add_row(
            result.year_month,
            str(result.flows),
            str(result.runs),
            str(result.run_logs),
            humanize.naturalsize(result.json_size),
            humanize.naturalsize(result.db_size),
        )
    console.print(table)
    console.print("The JSON files were kept, and are no longer used for migrated months.")


UPLOAD_LIST_LIMIT = 50


def upload_data(fc: flows_cache.FlowsCache, uploader, assume_yes: bool = False):
    """Show what a sync of the data folder will send, then submit it without waiting."""
    data_dir = fc.data_manager.basepath
    console.print(f"Comparing {data_dir} with the remote collection...")
    plan = uploader.plan_sync(data_dir)

    destination = uploader.get_collection_name(uploader.collection_id)
    stats = Table("", "", show_header=False, box=None)
    stats.add_row("Destination", f"{destination} ({uploader.collection_id})")
    stats.add_row("Path", uploader.base_path)
    stats.add_row("Local files", str(plan.local_files))
    stats.add_row(
        "Not synced",
        f"{len(plan.pending)} files ({humanize.naturalsize(plan.pending_size)})",
    )
    if plan.remote_only:
        stats.add_row("Remote only", f"{len(plan.remote_only)} files (left untouched)")
    console.print(stats)

    if not plan.pending:
        console.print("Remote collection is up to date, nothing to upload.")
        return

    table = Table("File", "Size", "Reason")
    for item in plan.pending[:UPLOAD_LIST_LIMIT]:
        table.add_row(
            str(item.path.relative_to(data_dir)),
            humanize.naturalsize(item.size),
            item.reason,
        )
    if len(plan.pending) > UPLOAD_LIST_LIMIT:
        table.add_row(f"... and {len(plan.pending) - UPLOAD_LIST_LIMIT} more", "", "")
    console.print(table)

    if not assume_yes and not typer.confirm(
        f"Upload {len(plan.pending)} files ({humanize.naturalsize(plan.pending_size)}) "
        f"to {destination}?"
    ):
        console.print("Upload cancelled.")
        return

    task_id = uploader.submit(data_dir, label=f"flow-info upload {fc.name}")
    console.print(f"Submitted transfer task {task_id}, follow it at {uploader.task_url(task_id)}")

@app.command()
def update(
    gui: bool = True,
    flows: bool = typer.Option(True, help="Update flows. --no-flows skips them."),
    runs: bool = typer.Option(True, help="Update runs. --no-runs skips them."),
    logs: bool = typer.Option(True, help="Update run logs. --no-logs skips them."),
    workers: int = 3,
):
    fc = get_flows_cache(get_config())
    fc.login()

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
            fc.update_run_logs(
                lambda year_month, done, total: console.print(
                    f"Downloading Run Logs {run_logs_label(year_month, done, total)}"
                ),
                workers=workers,
            )
        return

    with Progress() as progress:

        flows_task = progress.add_task("[red]Downloading Flows...")
        runs_task = progress.add_task("[green]Downloading Runs...")
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
            total = fc.update_run_logs(
                lambda year_month, done, total: progress.update(
                    run_logs_task,
                    completed=done,
                    total=total,
                    description="[cyan]Downloading Run Logs "
                    + run_logs_label(year_month, done, total),
                ),
                workers,
            )
            if not total:
                # Nothing to fetch, show the bar as finished rather than empty.
                progress.update(run_logs_task, completed=1, total=1)


def run_logs_label(year_month, done: int, total: int) -> str:
    """Run logs progress, with the month currently being fetched."""
    if not total:
        return "(none to fetch)"
    return f"{year_month or ''} ({done}/{total})".strip()


@app.command()
def upload(
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Start the transfer without asking for confirmation."
    ),
):
    """Sync the data folder to the configured remote_collection with Globus Transfer."""
    fc = get_flows_cache(get_config())
    # The uploader registers its scopes on the app, so it must exist before login.
    uploader = fc.get_uploader()
    uploader.login()
    upload_data(fc, uploader, assume_yes=yes)


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
    run_range: str = TYPER_OP_RUN_RANGE,
    all_runs: bool = TYPER_OP_ALL_RUNS,
    filter_transfer_states: t.List[str] = None,
):
    run_range = parse_run_range(run_range)

    fi = flow_info.FlowInfo(get_flows_cache(get_config()))
    # Track progress through iterations of logs
    list(track(fi.load(run_range=run_range, all_runs=all_runs)))
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
    run_range: str = TYPER_OP_RUN_RANGE,
    all_runs: bool = TYPER_OP_ALL_RUNS,
    compute_only: bool = False,
):
    """
    todo: Collect number of runs present in each step
    """
    fi = flow_info.FlowInfo(get_flows_cache(get_config()))
    list(track(fi.load(run_range=parse_run_range(run_range), all_runs=all_runs, step_times_compute_only=compute_only)))
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
def plot_step_times(
    run_range: str = TYPER_OP_RUN_RANGE,
    all_runs: bool = TYPER_OP_ALL_RUNS,
):
    config = get_config()
    fi = flow_info.FlowInfo(get_flows_cache(config))
    amount = len(list(track(fi.load(run_range=parse_run_range(run_range), all_runs=all_runs))))

    app = config["beamlines"]["current_app"]
    filename = plots.plot_step_times(fi.get_flow_stats(), name=app)
    console.print(f"Generated gantt with {amount} logs and saved to {filename}.")


@app.command()
def plot_gantt(
    run_range: str = TYPER_OP_RUN_RANGE,
    all_runs: bool = TYPER_OP_ALL_RUNS,
):
    config = get_config()
    fi = flow_info.FlowInfo(get_flows_cache(config))
    amount = len(list(track(fi.load(run_range=parse_run_range(run_range), all_runs=all_runs))))

    app = config["beamlines"]["current_app"]
    order = config["beamlines"][app]["flow_order"]
    filename = plots.plot_gantt(fi.get_flow_stats(), order, name=app)
    console.print(f"Generated gantt with {amount} logs and saved to {filename}.")


@app.callback()
def main(ctx: typer.Context, verbose: bool = False, log_level: str = "WARNING"):

    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        console.print(
            f"Invalid log level: {log_level}. Must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL."
        )
        raise typer.Exit(code=1)
    if verbose:
        log_level = "DEBUG"
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
                    "level": log_level,
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
    except exc.MigrationRequired as e:
        console.print(str(e))
    except exc.ConfigException as e:
        console.log(f"Config Error: {str(e)}")


if __name__ == "__main__":
    main_cli()
