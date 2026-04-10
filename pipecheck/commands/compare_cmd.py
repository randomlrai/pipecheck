"""CLI sub-command: compare two DAG files."""

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.comparator import DAGComparator, CompareError


def add_compare_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Register the 'compare' sub-command."""
    parser = subparsers.add_parser(
        "compare",
        help="Compare two DAG definition files and report differences.",
    )
    parser.add_argument("left", help="Path to the baseline DAG file.")
    parser.add_argument("right", help="Path to the DAG file to compare against.")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if differences are found.",
    )
    parser.set_defaults(func=compare_command)


def compare_command(args: argparse.Namespace) -> int:
    """Execute the compare sub-command.

    Returns:
        0 on success (or no differences when --exit-code not set),
        1 if differences found and --exit-code is set,
        2 on load/compare error.
    """
    loader = DAGLoader()
    try:
        left_dag = loader.load_from_file(args.left)
        right_dag = loader.load_from_file(args.right)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG files: {exc}", file=sys.stderr)
        return 2

    comparator = DAGComparator()
    try:
        result = comparator.compare(left_dag, right_dag)
    except CompareError as exc:
        print(f"Comparison error: {exc}", file=sys.stderr)
        return 2

    print(result)

    if result.has_differences and args.exit_code:
        return 1
    return 0
