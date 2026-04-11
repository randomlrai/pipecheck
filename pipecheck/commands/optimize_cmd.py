"""CLI sub-command: optimize — suggest DAG optimizations."""
from __future__ import annotations
import argparse
import sys
from pipecheck.formats import DAGLoader
from pipecheck.optimizer import DAGOptimizer


def add_optimize_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "optimize",
        help="Suggest parallelism and redundancy optimizations for a DAG.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when optimization hints are found.",
    )
    parser.set_defaults(func=optimize_command)


def optimize_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    result = DAGOptimizer().optimize(dag)
    print(str(result))

    if args.exit_code and result.has_hints:
        return 1
    return 0
