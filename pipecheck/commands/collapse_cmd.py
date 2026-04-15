"""CLI sub-command: collapse — merge linear task chains in a DAG."""
from __future__ import annotations
import argparse
import sys
from pipecheck.formats import DAGLoader
from pipecheck.collapser import DAGCollapser


def add_collapse_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "collapse",
        help="Merge linear task chains into single collapsed nodes.",
    )
    parser.add_argument("file", help="Path to DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--summary",
        action="store_true",
        default=False,
        help="Print a one-line summary instead of full details.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any chains were collapsed.",
    )
    parser.set_defaults(func=collapse_command)


def collapse_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # pragma: no cover
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 2

    collapser = DAGCollapser()
    result = collapser.collapse(dag)

    if args.summary:
        status = "collapsed" if result.has_collapses else "no changes"
        print(f"{dag.name}: {result.count} chain(s) collapsed ({status})")
    else:
        print(str(result))
        if result.has_collapses:
            print()
            print(f"Resulting DAG tasks ({len(result.dag.tasks)}):")
            for tid in sorted(result.dag.tasks):
                print(f"  {tid}")

    if args.exit_code and result.has_collapses:
        return 1
    return 0
