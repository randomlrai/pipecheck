"""CLI sub-command: match — find tasks by pattern."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.matcher import DAGMatcher, MatchError


def add_match_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "match",
        help="Find tasks whose IDs match a pattern.",
    )
    parser.add_argument("dag_file", help="Path to DAG definition file (JSON/YAML).")
    parser.add_argument("pattern", help="Pattern to match against task IDs.")
    parser.add_argument(
        "--mode",
        choices=["glob", "regex", "exact"],
        default="glob",
        help="Matching mode (default: glob).",
    )
    parser.add_argument(
        "--ids-only",
        action="store_true",
        default=False,
        help="Print only matched task IDs, one per line.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when no tasks match.",
    )
    parser.set_defaults(func=match_command)


def match_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        matcher = DAGMatcher(dag)
        result = matcher.match(args.pattern, mode=args.mode)
    except MatchError as exc:
        print(f"Match error: {exc}", file=sys.stderr)
        return 1

    if args.ids_only:
        for tid in result.task_ids:
            print(tid)
    else:
        print(result)

    if args.exit_code and not result.has_matches:
        return 1
    return 0
