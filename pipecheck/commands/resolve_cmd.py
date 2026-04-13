"""CLI sub-command: resolve — print topological task order."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.resolver import DAGResolver, ResolveError


def add_resolve_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "resolve",
        help="Print tasks in topological execution order.",
    )
    parser.add_argument("file", help="Path to DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--numbered",
        action="store_true",
        default=False,
        help="Prefix each task with its step number.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if resolution fails.",
    )
    parser.set_defaults(func=resolve_command)


def resolve_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 1

    try:
        result = DAGResolver(dag).resolve()
    except ResolveError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1 if args.exit_code else 0

    for idx, task_id in enumerate(result.order, start=1):
        if args.numbered:
            print(f"{idx:>3}. {task_id}")
        else:
            print(task_id)

    return 0
