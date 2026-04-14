"""CLI sub-command: critical-path — find the longest weighted path in a DAG."""
from __future__ import annotations
import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.tracer_path import CriticalPathTracer, CriticalPathError


def add_critical_path_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "critical-path",
        help="Find the longest weighted path through a DAG.",
    )
    parser.add_argument("file", help="Path to DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--weight",
        default="timeout",
        metavar="ATTR",
        help="Task metadata attribute to use as edge weight (default: timeout).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when total path weight exceeds --threshold.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        metavar="N",
        help="Weight threshold for non-zero exit code (requires --exit-code).",
    )
    parser.set_defaults(func=critical_path_command)


def critical_path_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 1

    try:
        tracer = CriticalPathTracer()
        path = tracer.trace(dag, weight_attr=args.weight)
    except CriticalPathError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    print(str(path))
    print(f"\nTask count : {path.length()}")
    print(f"Total weight: {path.total_weight:.2f}")

    if args.exit_code and args.threshold is not None:
        if path.total_weight > args.threshold:
            print(
                f"[warn] Total weight {path.total_weight:.2f} exceeds threshold {args.threshold:.2f}",
                file=sys.stderr,
            )
            return 1

    return 0
