"""CLI command: batch — group DAG tasks into parallel execution batches."""
from __future__ import annotations
import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.batcher import DAGBatcher, BatchError


def add_batch_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "batch",
        help="Group DAG tasks into parallel execution batches.",
    )
    parser.add_argument("file", help="Path to DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if no batches are produced.",
    )


def batch_command(args: argparse.Namespace) -> int:
    """Execute the batch command and return an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # pragma: no cover
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        batcher = DAGBatcher(dag)
        result = batcher.batch()
    except BatchError as exc:
        print(f"Batch error: {exc}", file=sys.stderr)
        return 1

    print(str(result))

    if args.exit_code and result.count == 0:
        return 1
    return 0
