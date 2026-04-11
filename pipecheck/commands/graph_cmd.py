"""CLI subcommand: graph — display structural metrics for a DAG."""
from __future__ import annotations
import argparse
import sys
from pipecheck.formats import DAGLoader
from pipecheck.grapher import DAGGrapher


def add_graph_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "graph",
        help="Compute and display graph metrics for a DAG file.",
    )
    parser.add_argument("dag_file", help="Path to the DAG JSON/YAML file.")
    parser.add_argument(
        "--reachable",
        metavar="TASK_ID",
        default=None,
        help="Show tasks reachable from the given task ID.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if the DAG has no root tasks.",
    )
    parser.set_defaults(func=graph_command)


def graph_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 1

    metrics = DAGGrapher(dag).analyze()
    print(str(metrics))

    if args.reachable:
        task_id = args.reachable
        if task_id not in metrics.reachable_from:
            print(f"[error] Task '{task_id}' not found in DAG.", file=sys.stderr)
            return 1
        reachable = sorted(metrics.reachable_from[task_id])
        if reachable:
            print(f"Reachable from '{task_id}': {', '.join(reachable)}")
        else:
            print(f"No tasks reachable from '{task_id}'.")

    if args.exit_code and not metrics.root_tasks:
        return 1
    return 0
