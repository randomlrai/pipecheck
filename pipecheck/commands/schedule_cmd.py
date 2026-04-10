"""CLI sub-command: schedule — show execution waves and critical path."""
from __future__ import annotations
import argparse
from pipecheck.formats import DAGLoader
from pipecheck.scheduler import DAGScheduler


def add_schedule_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "schedule",
        help="Display the execution wave schedule and critical path for a DAG.",
    )
    parser.add_argument("file", help="Path to DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--critical-path",
        action="store_true",
        default=False,
        help="Also print the critical path through the DAG.",
    )
    parser.add_argument(
        "--waves-only",
        action="store_true",
        default=False,
        help="Print only the wave list, suppress the header.",
    )
    parser.set_defaults(func=schedule_command)


def schedule_command(args: argparse.Namespace) -> int:
    """Execute the schedule sub-command. Returns exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}")
        return 1

    scheduler = DAGScheduler(dag)

    try:
        schedule = scheduler.build_schedule()
    except Exception as exc:  # noqa: BLE001
        print(f"Error building schedule: {exc}")
        return 1

    if args.waves_only:
        for wave in schedule.waves:
            print(wave)
    else:
        print(schedule)

    if args.critical_path:
        path = scheduler.critical_path()
        print("\nCritical path: " + " -> ".join(path))

    return 0
