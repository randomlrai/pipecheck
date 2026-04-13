"""CLI sub-command: pipecheck index — index DAG tasks by a metadata attribute."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.indexer import DAGIndexer, IndexError as DAGIndexError


def add_index_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "index",
        help="Index DAG tasks by a metadata attribute",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file")
    parser.add_argument(
        "attribute",
        help="Metadata attribute to index by (e.g. owner, team, env, version)",
    )
    parser.add_argument(
        "--value",
        default=None,
        help="Filter output to tasks matching this value",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when no entries are found",
    )
    parser.set_defaults(func=index_command)


def index_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        indexer = DAGIndexer()
        result = indexer.index(dag, args.attribute)
    except DAGIndexError as exc:
        print(f"Index error: {exc}", file=sys.stderr)
        return 1

    if args.value:
        matched = result.tasks_for_value(args.value)
        if matched:
            print(f"Tasks where {args.attribute}={args.value!r}:")
            for task_id in sorted(matched):
                print(f"  {task_id}")
        else:
            print(f"No tasks found where {args.attribute}={args.value!r}")
        if args.exit_code and not matched:
            return 1
        return 0

    print(str(result))
    if args.exit_code and not result.has_entries():
        return 1
    return 0
