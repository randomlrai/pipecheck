"""CLI subcommand: group — partition DAG tasks into named groups."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.grouper import DAGGrouper, GroupError


def add_group_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "group",
        help="Partition DAG tasks into named groups by tag or prefix.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--by",
        choices=["tag", "prefix"],
        default="tag",
        help="Grouping strategy: 'tag' (default) or 'prefix'.",
    )
    parser.add_argument(
        "--separator",
        default="_",
        help="Separator used for prefix grouping (default: '_').",
    )
    parser.set_defaults(func=group_command)


def group_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # pragma: no cover
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    grouper = DAGGrouper()
    try:
        if args.by == "tag":
            result = grouper.by_tag(dag)
        else:
            result = grouper.by_prefix(dag, separator=args.separator)
    except GroupError as exc:
        print(f"Grouping error: {exc}", file=sys.stderr)
        return 1

    print(result)
    return 0
