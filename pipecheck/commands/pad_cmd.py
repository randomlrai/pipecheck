"""CLI sub-command: pipecheck pad — insert placeholder tasks into a DAG."""
from __future__ import annotations
import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.padder import DAGPadder, PadError


def add_pad_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "pad",
        help="Insert no-op placeholder tasks to fill structural gaps in the DAG.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--max-depth",
        type=int,
        default=1,
        dest="max_depth",
        help="Maximum allowed edge span before a pad is inserted (default: 1).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        dest="exit_code",
        help="Exit with code 1 if any pads were inserted.",
    )
    parser.set_defaults(func=pad_command)


def pad_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:
        print(f"[error] Failed to load DAG: {exc}", file=sys.stderr)
        return 1

    try:
        padder = DAGPadder(dag, max_depth=args.max_depth)
    except PadError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    result = padder.pad()
    print(str(result))

    if args.exit_code and result.has_pads():
        return 1
    return 0
