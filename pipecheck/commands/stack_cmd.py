"""CLI subcommand: pipecheck stack — show topological layers of a DAG."""
from __future__ import annotations
import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.stacker import DAGStacker, StackError


def add_stack_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "stack",
        help="Display tasks grouped into topological layers.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--show-widest",
        action="store_true",
        default=False,
        help="Highlight the widest layer.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if the DAG cannot be stacked (e.g. cycle).",
    )
    parser.set_defaults(func=stack_command)


def stack_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Failed to load DAG: {exc}", file=sys.stderr)
        return 1

    try:
        result = DAGStacker().stack(dag)
    except StackError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1 if args.exit_code else 0

    print(str(result))

    if args.show_widest and result.widest_layer is not None:
        wl = result.widest_layer
        print(f"\nWidest layer: Layer {wl.index} ({len(wl)} task(s))")

    return 0
