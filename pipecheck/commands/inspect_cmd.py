"""CLI sub-command: inspect — show per-task details for a DAG file."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader, FormatError
from pipecheck.inspector import DAGInspector


def add_inspect_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "inspect",
        help="Inspect tasks and dependency metadata in a DAG file.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--task",
        metavar="TASK_ID",
        default=None,
        help="Inspect a single task by ID instead of the whole DAG.",
    )
    parser.add_argument(
        "--depth",
        action="store_true",
        default=False,
        help="Show only depth values for all tasks.",
    )
    parser.set_defaults(func=inspect_command)


def inspect_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except (FormatError, FileNotFoundError) as exc:
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    inspector = DAGInspector(dag)
    report = inspector.inspect()

    if args.depth:
        for task_id, inspection in sorted(report.tasks.items()):
            print(f"{task_id}: depth={inspection.depth}")
        return 0

    if args.task:
        inspection = report.get(args.task)
        if inspection is None:
            print(f"Task '{args.task}' not found in DAG.", file=sys.stderr)
            return 1
        print(inspection)
        return 0

    for task_id in sorted(report.tasks):
        print(report.tasks[task_id])
        print()
    return 0
