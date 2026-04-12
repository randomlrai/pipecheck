"""CLI sub-command: filter tasks in a DAG."""
from __future__ import annotations
import argparse
import sys
from pipecheck.formats import DAGLoader
from pipecheck.filterer import DAGFilterer, FilterError


def add_filter_subparser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("filter", help="Filter DAG tasks by criteria")
    p.add_argument("dag_file", help="Path to DAG JSON/YAML file")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--tag", metavar="TAG",
                       help="Keep tasks that carry this tag")
    group.add_argument("--prefix", metavar="PREFIX",
                       help="Keep tasks whose ID starts with PREFIX")
    group.add_argument("--max-timeout", type=int, metavar="SECONDS",
                       help="Keep tasks with timeout <= SECONDS")
    group.add_argument("--no-timeout", action="store_true",
                       help="Keep tasks that have no timeout set")
    p.add_argument("--quiet", action="store_true",
                   help="Suppress output; use exit code only")
    p.set_defaults(func=filter_command)


def filter_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    filterer = DAGFilterer(dag)

    try:
        if args.tag:
            result = filterer.by_tag(args.tag)
        elif args.prefix:
            result = filterer.by_prefix(args.prefix)
        elif args.max_timeout is not None:
            result = filterer.by_max_timeout(args.max_timeout)
        else:
            result = filterer.by_no_timeout()
    except FilterError as exc:
        print(f"Filter error: {exc}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(result)

    return 0 if result.has_matches else 2
