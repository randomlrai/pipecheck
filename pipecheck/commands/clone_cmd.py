"""CLI sub-command: clone a DAG definition."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.cloner import CloneError, DAGCloner
from pipecheck.formats import DAGLoader, FormatError


def add_clone_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    """Register the *clone* sub-command."""
    parser = subparsers.add_parser(
        "clone",
        help="Clone a DAG definition with an optional task prefix.",
    )
    parser.add_argument("file", help="Path to the DAG file (JSON or YAML).")
    parser.add_argument("new_name", help="Name for the cloned DAG.")
    parser.add_argument(
        "--prefix",
        default=None,
        metavar="PREFIX",
        help="String prepended to every task id in the clone.",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help="Write cloned DAG as JSON to FILE (default: stdout).",
    )
    parser.set_defaults(func=clone_command)


def clone_command(args: argparse.Namespace) -> int:
    """Execute the clone sub-command.

    Returns:
        0 on success, 1 on error.
    """
    try:
        dag = DAGLoader.load_from_file(args.file)
    except (FormatError, FileNotFoundError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    cloner = DAGCloner()
    try:
        result = cloner.clone(dag, new_name=args.new_name, prefix=args.prefix)
    except CloneError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    payload = {
        "name": result.cloned_dag.name,
        "tasks": [
            {
                "task_id": t.task_id,
                "description": t.description,
                "timeout": t.timeout,
                "tags": sorted(t.tags),
                "dependencies": sorted(t.dependencies),
                "metadata": t.metadata,
            }
            for t in result.cloned_dag.tasks.values()
        ],
    }
    serialised = json.dumps(payload, indent=2)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(serialised)
        print(str(result))
    else:
        print(serialised)

    return 0
