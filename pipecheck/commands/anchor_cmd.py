"""CLI sub-command: anchor — mark DAG tasks as fixed reference points."""
from __future__ import annotations

import argparse
import sys

from pipecheck.anchorer import AnchorError, DAGAnchorer
from pipecheck.formats import DAGLoader, FormatError


def add_anchor_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "anchor",
        help="Mark one or more tasks as anchored reference points in a DAG.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "task_ids",
        nargs="+",
        metavar="TASK_ID",
        help="One or more task IDs to anchor.",
    )
    parser.add_argument(
        "--reason",
        default=None,
        help="Optional reason for anchoring.",
    )
    parser.add_argument(
        "--version",
        dest="pinned_version",
        default=None,
        help="Optional pinned version string to attach to each anchor.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when no anchors were applied.",
    )
    parser.set_defaults(func=anchor_command)


def anchor_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except FormatError as exc:
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    anchorer = DAGAnchorer(dag)

    for task_id in args.task_ids:
        try:
            anchorer.anchor(
                task_id,
                reason=args.reason,
                pinned_version=args.pinned_version,
            )
        except AnchorError as exc:
            print(f"Warning: {exc}", file=sys.stderr)

    result = anchorer.build_result()
    print(result)

    if args.exit_code and not result.has_anchors():
        return 1
    return 0
