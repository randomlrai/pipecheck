"""CLI sub-command: split a DAG file into connected-component sub-DAGs."""

from __future__ import annotations

import argparse
import json

from pipecheck.formats import DAGLoader
from pipecheck.splitter import DAGSplitter
from pipecheck.visualizer import DAGVisualizer


def add_split_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "split",
        help="Split a DAG into independent connected-component sub-DAGs.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--format",
        choices=["text", "mermaid", "json"],
        default="text",
        help="Output format for each sub-DAG (default: text).",
    )
    parser.add_argument(
        "--min-parts",
        type=int,
        default=1,
        metavar="N",
        help="Exit with code 1 if fewer than N parts are produced.",
    )
    parser.set_defaults(func=split_command)


def split_command(args: argparse.Namespace) -> int:
    """Execute the split sub-command. Returns an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}")
        return 1

    splitter = DAGSplitter()
    try:
        result = splitter.split(dag)
    except Exception as exc:  # noqa: BLE001
        print(f"Error splitting DAG: {exc}")
        return 1

    print(f"Split '{result.source_name}' into {result.count} part(s).\n")

    for i, part in enumerate(result.parts, start=1):
        print(f"--- Part {i}: {part.name} ({len(part.tasks)} task(s)) ---")
        if args.format == "mermaid":
            viz = DAGVisualizer(part)
            print(viz.to_mermaid())
        elif args.format == "json":
            viz = DAGVisualizer(part)
            print(json.dumps(viz.to_json(), indent=2))
        else:
            for tid in part.tasks:
                deps = part.dependencies.get(tid, [])
                dep_str = f" -> {', '.join(deps)}" if deps else ""
                print(f"  {tid}{dep_str}")
        print()

    if result.count < args.min_parts:
        print(
            f"Expected at least {args.min_parts} part(s), got {result.count}."
        )
        return 1

    return 0
