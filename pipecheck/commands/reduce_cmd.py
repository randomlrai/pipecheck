"""CLI sub-command: reduce — collapse linear chains in a DAG."""

import argparse
import json

from pipecheck.formats import DAGLoader
from pipecheck.reducer import DAGReducer, ReduceError
from pipecheck.visualizer import DAGVisualizer


def add_reduce_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "reduce",
        help="Collapse linear task chains into compound tasks.",
    )
    parser.add_argument("dag_file", help="Path to the DAG JSON/YAML file.")
    parser.add_argument(
        "--format",
        choices=["text", "mermaid", "dot", "json"],
        default="text",
        help="Output format for the reduced DAG (default: text).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when at least one reduction was made.",
    )
    parser.set_defaults(func=reduce_command)


def reduce_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}")
        return 1

    try:
        result = DAGReducer().reduce(dag)
    except ReduceError as exc:
        print(f"Reduce error: {exc}")
        return 1

    fmt = getattr(args, "format", "text")

    if fmt == "text":
        print(str(result))
    elif fmt == "mermaid":
        print(DAGVisualizer(result.dag).to_mermaid())
    elif fmt == "dot":
        print(DAGVisualizer(result.dag).to_dot())
    elif fmt == "json":
        data = {
            "dag": result.dag.name,
            "reductions": [
                {"original": e.original_ids, "compound": e.compound_id}
                for e in result.entries
            ],
        }
        print(json.dumps(data, indent=2))

    if getattr(args, "exit_code", False) and result.has_reductions:
        return 1
    return 0
