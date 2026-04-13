"""CLI sub-command: catalog — list all tasks in a DAG with their metadata."""
from __future__ import annotations

import argparse
import sys

from pipecheck.cataloger import DAGCataloger
from pipecheck.formats import DAGLoader


def add_catalog_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "catalog",
        help="Print a catalog of all tasks in the DAG.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--task",
        metavar="TASK_ID",
        default=None,
        help="Show catalog entry for a single task only.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when the DAG has no tasks.",
    )


def catalog_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # pragma: no cover
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    cataloger = DAGCataloger()
    result = cataloger.catalog(dag)

    if args.task:
        entry = result.get(args.task)
        if entry is None:
            print(f"Task '{args.task}' not found in catalog.", file=sys.stderr)
            return 1
        print(str(entry))
    else:
        print(str(result))

    if args.exit_code and result.count == 0:
        return 1
    return 0
