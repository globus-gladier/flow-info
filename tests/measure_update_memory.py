"""Run `flow-info update` and report its peak memory use.

Samples the RSS of the process and all of its children with psutil while the
command runs. Anything after `--` replaces the default command, so this can
measure other subcommands too.

    python scripts/measure_update_memory.py
    python scripts/measure_update_memory.py --interval 0.1 --csv mem.csv
    python scripts/measure_update_memory.py -- flow-info update --no-gui --logs
"""

import argparse
import csv
import subprocess
import sys
import time

import humanize
import psutil

DEFAULT_COMMAND = ["flow-info", "runtimes", "--run-range", "2026-09--2026-11"]


def total_rss(proc: psutil.Process) -> int:
    """RSS of proc plus all descendants, in bytes."""
    total = 0
    for p in [proc, *proc.children(recursive=True)]:
        try:
            total += p.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total


def run(command: list[str], interval: float, csv_path: str | None) -> int:
    start = time.monotonic()
    child = subprocess.Popen(command)
    proc = psutil.Process(child.pid)
    samples = []
    peak = 0
    while child.poll() is None:
        rss = total_rss(proc)
        peak = max(peak, rss)
        samples.append((time.monotonic() - start, rss))
        time.sleep(interval)
    elapsed = time.monotonic() - start

    print(f"\nCommand:   {' '.join(command)}", file=sys.stderr)
    print(f"Exit code: {child.returncode}", file=sys.stderr)
    print(f"Elapsed:   {elapsed:.1f}s", file=sys.stderr)
    print(f"Peak RSS:  {humanize.naturalsize(peak, binary=True)}", file=sys.stderr)

    if csv_path:
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["seconds", "rss_bytes"])
            writer.writerows(samples)
        print(f"Samples:   {len(samples)} written to {csv_path}", file=sys.stderr)
    return child.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--interval", type=float, default=0.25, help="Seconds between samples.")
    parser.add_argument("--csv", help="Write every sample to this CSV file.")
    parser.add_argument("command", nargs="*", help="Command to run, after `--`.")
    args = parser.parse_args()
    sys.exit(run(args.command or DEFAULT_COMMAND, args.interval, args.csv))


if __name__ == "__main__":
    main()
