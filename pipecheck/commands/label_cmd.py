"""CLI sub-command: pipecheck label"""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.formats import DAGLoader
from pipecheck.labeler import DAGLabeler, LabelError


def add_label_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "label",
        help="Attach display labels to DAG tasks.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file.")
    parser.add_argument(
        "--labels",
        required=True,
        metavar="JSON",
        help='JSON object mapping task_id to label, e.g. \'{"a": "Ingest"}\'\'.',
    )
    parser.add_argument(
        "--colors",
        metavar="JSON",
        default=None,
        help="Optional JSON object mapping task_id to color hint.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.set_defaults(func=label_command)


def label_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader().load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        label_map: dict = json.loads(args.labels)
    except json.JSONDecodeError as exc:
        print(f"Invalid --labels JSON: {exc}", file=sys.stderr)
        return 1

    color_map: dict = {}
    if args.colors:
        try:
            color_map = json.loads(args.colors)
        except json.JSONDecodeError as exc:
            print(f"Invalid --colors JSON: {exc}", file=sys.stderr)
            return 1

    try:
        result = DAGLabeler().label(dag, label_map, color_map)
    except LabelError as exc:
        print(f"Label error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        payload = {
            "dag": result.dag_name,
            "labels": [
                {"task_id": e.task_id, "label": e.label, "color": e.color}
                for e in result.entries
            ],
        }
        print(json.dumps(payload, indent=2))
    else:
        print(str(result))

    return 0
