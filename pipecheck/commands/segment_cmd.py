"""CLI sub-command: segment — split a DAG into depth-based segments."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.segmenter import DAGSegmenter, SegmentError


def add_segment_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "segment",
        help="Split a DAG into depth-based segments.",
    )
    parser.add_argument("file", help="Path to the DAG file (JSON or YAML).")
    parser.add_argument(
        "--size",
        type=int,
        default=1,
        metavar="N",
        help="Number of depth levels per segment (default: 1).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when no segments are produced.",
    )
    parser.set_defaults(func=segment_command)


def segment_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        result = DAGSegmenter().segment(dag, size=args.size)
    except SegmentError as exc:
        print(f"Segment error: {exc}", file=sys.stderr)
        return 1

    print(str(result))

    if args.exit_code and result.count == 0:
        return 1
    return 0
