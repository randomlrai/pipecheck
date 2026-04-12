"""CLI sub-command: pipecheck scope — list tasks in a named scope."""
from __future__ import annotations
import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.scoper import DAGScoper, ScopeError


def add_scope_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("scope", help="List tasks belonging to a named scope.")
    p.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    p.add_argument("scope_name", help="Scope label to query.")
    p.add_argument(
        "--list-all",
        action="store_true",
        default=False,
        help="Instead of querying a scope, list all scope labels in the DAG.",
    )
    p.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when no tasks are found for the requested scope.",
    )


def scope_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # pragma: no cover
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    scoper = DAGScoper(dag)

    if getattr(args, "list_all", False):
        labels = scoper.all_scopes()
        if labels:
            print("Scopes defined in DAG '{}':".format(dag.name))
            for label in labels:
                print(f"  {label}")
        else:
            print(f"No scopes defined in DAG '{dag.name}'.")
        return 0

    try:
        result = scoper.scope(args.scope_name)
    except ScopeError as exc:
        print(f"Scope error: {exc}", file=sys.stderr)
        return 1

    print(result)

    if args.exit_code and not result.has_tasks:
        return 1
    return 0
