from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.tracer import DAGTracer


def add_trace_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "trace",
        help="Trace execution order from a start task",
    )
    parser.add_argument("file", help="Path to DAG definition file (JSON or YAML)")
    parser.add_argument("start", help="Task ID to start tracing from")
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        dest="max_depth",
        help="Maximum traversal depth (default: unlimited)",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        dest="exit_code",
        help="Exit with non-zero code if no steps found",
    )


def trace_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    tracer = DAGTracer(dag)

    try:
        result = tracer.trace(args.start, max_depth=args.max_depth)
    except Exception as exc:  # noqa: BLE001
        print(f"Error tracing DAG: {exc}", file=sys.stderr)
        return 1

    print(str(result))

    if args.exit_code and not result.task_ids:
        return 1

    return 0
