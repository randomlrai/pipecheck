"""CLI subcommand: route — find all paths between two tasks."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.router import DAGRouter, RouteError


def add_route_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "route",
        help="Find all paths between two tasks in a DAG.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument("source", help="Source task ID.")
    parser.add_argument("target", help="Target task ID.")
    parser.add_argument(
        "--shortest",
        action="store_true",
        default=False,
        help="Print only the shortest path.",
    )
    parser.add_argument(
        "--longest",
        action="store_true",
        default=False,
        help="Print only the longest path.",
    )
    parser.set_defaults(func=route_command)


def route_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        router = DAGRouter(dag)
        result = router.route(args.source, args.target)
    except RouteError as exc:
        print(f"Route error: {exc}", file=sys.stderr)
        return 1

    if not result.has_routes():
        print(f"No path found from '{args.source}' to '{args.target}'.")
        return 0

    if args.shortest:
        route = result.shortest()
        print(f"Shortest path: {route}")
    elif args.longest:
        route = result.longest()
        print(f"Longest path: {route}")
    else:
        print(result)

    return 0
