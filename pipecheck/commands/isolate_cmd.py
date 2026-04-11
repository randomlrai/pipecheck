"""CLI sub-command: isolate — extract a reachable subgraph from a DAG."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.formats import DAGLoader
from pipecheck.isolator import DAGIsolator, IsolateError


def add_isolate_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "isolate",
        help="Extract the subgraph reachable from one or more root tasks.",
    )
    parser.add_argument("dag_file", help="Path to the DAG JSON/YAML file.")
    parser.add_argument(
        "roots",
        nargs="+",
        metavar="TASK_ID",
        help="One or more root task IDs to isolate from.",
    )
    parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        default=False,
        help="Output result as JSON.",
    )
    parser.set_defaults(func=isolate_command)


def isolate_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    isolator = DAGIsolator()
    try:
        result = isolator.isolate(dag, args.roots)
    except IsolateError as exc:
        print(f"Isolation error: {exc}", file=sys.stderr)
        return 1

    if args.output_json:
        payload = {
            "original_dag": result.original_name,
            "roots": result.roots,
            "included_tasks": result.included_task_ids,
            "task_count": result.count,
        }
        print(json.dumps(payload, indent=2))
    else:
        print(f"Isolated subgraph of '{result.original_name}'")
        print(f"  Roots : {', '.join(result.roots)}")
        print(f"  Tasks ({result.count}): {', '.join(result.included_task_ids)}")

    return 0
