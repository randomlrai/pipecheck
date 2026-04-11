"""CLI sub-command: partition — split a DAG into named groups."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.partitioner import DAGPartitioner


def add_partition_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "partition",
        help="Partition a DAG by tag or metadata key.",
    )
    parser.add_argument("file", help="Path to the DAG file (JSON or YAML).")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--by-tag",
        action="store_true",
        default=True,
        help="Partition tasks by their 'tags' metadata list (default).",
    )
    group.add_argument(
        "--by-key",
        metavar="KEY",
        dest="by_key",
        default=None,
        help="Partition tasks by the value of a metadata key.",
    )
    parser.add_argument(
        "--show-unassigned",
        action="store_true",
        default=False,
        help="Print tasks that could not be assigned to any partition.",
    )
    parser.set_defaults(func=partition_command)


def partition_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    partitioner = DAGPartitioner(dag)

    if args.by_key:
        result = partitioner.by_metadata_key(args.by_key)
    else:
        result = partitioner.by_tag()

    print(str(result))

    if args.show_unassigned and result.has_unassigned():
        ids = ", ".join(t.task_id for t in result.unassigned)
        print(f"\nUnassigned tasks: {ids}")

    return 0
