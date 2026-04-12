"""CLI sub-command: pipecheck cut."""
from __future__ import annotations

import argparse
import sys

from pipecheck.cutter import CutError, DAGCutter
from pipecheck.formats import DAGLoader


def add_cut_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "cut",
        help="Show upstream/downstream halves when cutting at a task boundary.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument("task", help="Task ID to cut at.")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when the cut task has no upstream or no downstream.",
    )
    parser.set_defaults(func=cut_command)


def cut_command(args: argparse.Namespace) -> int:
    """Execute the cut sub-command. Returns an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 1

    try:
        cutter = DAGCutter()
        result = cutter.cut(dag, args.task)
    except CutError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    print(str(result))

    if result.has_upstream:
        print("\nUpstream tasks:")
        for task in result.upstream:
            print(f"  - {task.task_id}")
    else:
        print("\n(no upstream tasks)")

    if result.has_downstream:
        print("\nDownstream tasks:")
        for task in result.downstream:
            print(f"  - {task.task_id}")
    else:
        print("\n(no downstream tasks)")

    if args.exit_code and (not result.has_upstream or not result.has_downstream):
        return 1
    return 0
