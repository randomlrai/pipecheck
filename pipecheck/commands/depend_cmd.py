"""CLI subcommand: depend — show task dependencies."""
from __future__ import annotations
import argparse
import sys
from pipecheck.formats import DAGLoader
from pipecheck.depender import DAGDepender, DependError


def add_depend_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "depend",
        help="Show upstream/downstream dependencies for a task.",
    )
    parser.add_argument("file", help="Path to the DAG definition file.")
    parser.add_argument("task", help="Task ID to analyse.")
    parser.add_argument(
        "--ancestors-only",
        action="store_true",
        default=False,
        help="Print only ancestor task IDs, one per line.",
    )
    parser.add_argument(
        "--descendants-only",
        action="store_true",
        default=False,
        help="Print only descendant task IDs, one per line.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit 1 if the task has no dependencies at all.",
    )
    parser.set_defaults(func=depend_command)


def depend_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        depender = DAGDepender(dag)
        result = depender.analyze(args.task)
    except DependError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.ancestors_only:
        for tid in result.all_ancestors:
            print(tid)
    elif args.descendants_only:
        for tid in result.all_descendants:
            print(tid)
    else:
        print(result)

    if args.exit_code:
        has_any = bool(result.upstream or result.downstream)
        return 0 if has_any else 1

    return 0
