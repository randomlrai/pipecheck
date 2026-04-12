"""CLI sub-command: mark — attach markers to DAG tasks."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.marker import DAGMarker, MarkerError, VALID_MARKERS


def add_mark_subparser(subparsers) -> None:
    parser = subparsers.add_parser(
        "mark",
        help="Attach a marker (skip/force/deprecated/experimental) to a task",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file")
    parser.add_argument("task_id", help="ID of the task to mark")
    parser.add_argument(
        "--marker",
        choices=sorted(VALID_MARKERS),
        required=True,
        help="Marker to apply",
    )
    parser.add_argument("--reason", default=None, help="Optional reason for the marker")
    parser.add_argument(
        "--all-markers",
        action="store_true",
        default=False,
        help="List all markers applied to the DAG instead of marking a task",
    )
    parser.set_defaults(func=mark_command)


def mark_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    marker_obj = DAGMarker(dag)

    if args.all_markers:
        result = marker_obj.build()
        print(result)
        return 0

    try:
        entry = marker_obj.mark(args.task_id, args.marker, reason=args.reason)
    except MarkerError as exc:
        print(f"Marker error: {exc}", file=sys.stderr)
        return 1

    print(entry)
    return 0
