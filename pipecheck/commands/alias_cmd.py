"""CLI sub-command: alias — apply human-friendly aliases to DAG task IDs."""

from __future__ import annotations

import argparse
import json
import sys

from pipecheck.formats import DAGLoader
from pipecheck.aliaser import DAGAliaser


def add_alias_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "alias",
        help="Map task IDs to human-friendly aliases.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--map",
        metavar="TASK_ID=ALIAS",
        nargs="+",
        required=True,
        help="One or more task_id=alias pairs.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Exit with code 1 if any task IDs are unresolved.",
    )
    parser.set_defaults(func=alias_command)


def alias_command(args: argparse.Namespace) -> int:
    """Execute the alias sub-command. Returns an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    mapping: dict[str, str] = {}
    for pair in args.map:
        if "=" not in pair:
            print(f"Invalid mapping '{pair}': expected TASK_ID=ALIAS format.", file=sys.stderr)
            return 1
        task_id, _, alias = pair.partition("=")
        mapping[task_id.strip()] = alias.strip()

    aliaser = DAGAliaser()
    try:
        result = aliaser.alias(dag, mapping)
    except Exception as exc:  # noqa: BLE001
        print(f"Alias error: {exc}", file=sys.stderr)
        return 1

    print(str(result))

    if args.strict and result.has_unresolved:
        return 1
    return 0
