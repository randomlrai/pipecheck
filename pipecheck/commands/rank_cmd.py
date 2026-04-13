"""CLI sub-command: rank tasks in a DAG by a chosen metric."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.ranker import DAGRanker, RankError


def add_rank_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "rank",
        help="Rank tasks by degree, depth, or timeout.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--metric",
        choices=["degree", "depth", "timeout"],
        default="degree",
        help="Metric to rank tasks by (default: degree).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        metavar="N",
        help="Show only the top N results (0 = all).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if the DAG has no tasks.",
    )
    parser.set_defaults(func=rank_command)


def rank_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        ranker = DAGRanker()
        result = ranker.rank(dag, metric=args.metric)
    except RankError as exc:
        print(f"Rank error: {exc}", file=sys.stderr)
        return 1

    entries = result.top(args.top) if args.top > 0 else result.entries

    print(f"Ranking DAG '{result.dag_name}' by {result.metric}:")
    if not entries:
        print("  (no tasks)")
    else:
        for entry in entries:
            print(f"  {entry}")

    if args.exit_code and result.count == 0:
        return 1
    return 0
