"""CLI sub-command: reachability — analyse task reachability in a DAG."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.reachability import DAGReachabilityAnalyzer, ReachabilityError


def add_reachability_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "reachability",
        help="Show which tasks each task can transitively reach.",
    )
    parser.add_argument("dag_file", help="Path to the DAG JSON/YAML file.")
    parser.add_argument(
        "--from",
        dest="source",
        metavar="TASK_ID",
        help="Show only tasks reachable from TASK_ID.",
    )
    parser.add_argument(
        "--to",
        dest="target",
        metavar="TASK_ID",
        help="Check whether --from can reach TASK_ID (requires --from).",
    )
    parser.set_defaults(func=reachability_command)


def reachability_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        result = DAGReachabilityAnalyzer(dag).analyze()
    except ReachabilityError as exc:
        print(f"Reachability error: {exc}", file=sys.stderr)
        return 1

    if args.source and args.target:
        reachable = result.can_reach(args.source, args.target)
        status = "CAN" if reachable else "CANNOT"
        print(f"{args.source} {status} reach {args.target}")
        return 0 if reachable else 2

    if args.source:
        targets = sorted(result.reachable.get(args.source, frozenset()))
        if targets:
            print(f"Tasks reachable from '{args.source}':")
            for t in targets:
                print(f"  {t}")
        else:
            print(f"'{args.source}' cannot reach any other task.")
        return 0

    print(str(result))
    return 0
