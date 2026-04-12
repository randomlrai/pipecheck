"""CLI sub-command: block — mark DAG tasks as blocked."""

from __future__ import annotations

import argparse
import sys

from pipecheck.blocker import BlockError, DAGBlocker
from pipecheck.formats import DAGLoader, FormatError


def add_block_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "block",
        help="Mark one or more tasks as blocked and report cascading impacts.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "task_ids",
        nargs="+",
        metavar="TASK_ID",
        help="One or more task IDs to block.",
    )
    parser.add_argument(
        "--reason",
        default=None,
        metavar="TEXT",
        help="Optional reason for blocking.",
    )
    parser.add_argument(
        "--cascade",
        action="store_true",
        default=False,
        help="Also block all downstream dependents of the specified tasks.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any tasks are blocked.",
    )
    parser.set_defaults(func=block_command)


def block_command(args: argparse.Namespace) -> None:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except (FormatError, FileNotFoundError) as exc:
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        sys.exit(1)

    blocker = DAGBlocker(dag)
    try:
        result = blocker.block(
            task_ids=args.task_ids,
            reason=args.reason,
            cascade=args.cascade,
        )
    except BlockError as exc:
        print(f"Block error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(result)

    if args.exit_code and result.has_blocks():
        sys.exit(1)
