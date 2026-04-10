"""CLI sub-command: flatten — collapse a DAG into a depth-annotated task list."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.flattener import DAGFlattener, FlattenError


def add_flatten_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    """Register the *flatten* sub-command."""
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "flatten",
        help="Flatten a DAG into a single-level, depth-annotated task list.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--show-metadata",
        action="store_true",
        default=False,
        help="Include task metadata in the output.",
    )
    parser.set_defaults(func=flatten_command)


def flatten_command(args: argparse.Namespace) -> int:
    """Execute the flatten sub-command. Returns an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        result = DAGFlattener().flatten(dag)
    except FlattenError as exc:
        print(f"Flatten error: {exc}", file=sys.stderr)
        return 1

    print(f"Flattened DAG: {result.dag_name} ({len(result.tasks)} tasks)")
    for ft in result.tasks:
        line = f"  [depth={ft.depth}] {ft.task_id}"
        if ft.all_upstream:
            line += f"  upstream: {', '.join(ft.all_upstream)}"
        if args.show_metadata and ft.metadata:
            line += f"  meta: {ft.metadata}"
        print(line)

    return 0
