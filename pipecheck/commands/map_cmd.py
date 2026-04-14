"""CLI sub-command: map — display task dependency maps for a DAG."""
from __future__ import annotations

import argparse

from pipecheck.formats import DAGLoader
from pipecheck.mapper import DAGMapper


def add_map_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "map",
        help="Show upstream/downstream dependency map for every task in a DAG.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--task",
        metavar="TASK_ID",
        default=None,
        help="Limit output to a single task.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if the DAG contains no tasks.",
    )
    parser.set_defaults(func=map_command)


def map_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}")
        return 1

    mapper = DAGMapper()
    result = mapper.map(dag)

    if args.task:
        try:
            entry = result.get(args.task)
        except Exception as exc:  # noqa: BLE001
            print(f"Error: {exc}")
            return 1
        print(entry)
    else:
        print(result)

    if args.exit_code and len(result) == 0:
        return 1
    return 0
