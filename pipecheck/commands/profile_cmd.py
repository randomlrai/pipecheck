"""CLI sub-command: profile — analyse and display DAG performance profile."""

import argparse
import sys

from pipecheck.formats import DAGLoader, FormatError
from pipecheck.profiler import DAGProfiler


def add_profile_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "profile",
        help="Profile a DAG and report depth, critical path, and bottlenecks.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--bottlenecks-only",
        action="store_true",
        default=False,
        help="Print only bottleneck task IDs, one per line.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any bottlenecks are found.",
    )
    parser.set_defaults(func=profile_command)


def profile_command(args: argparse.Namespace) -> int:
    """Execute the profile sub-command. Returns an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except (FormatError, FileNotFoundError) as exc:
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    profiler = DAGProfiler(dag)
    result = profiler.profile()

    if args.bottlenecks_only:
        for tid in result.bottlenecks:
            print(tid)
    else:
        print(result.summary())

    if args.exit_code and result.bottlenecks:
        return 1
    return 0
