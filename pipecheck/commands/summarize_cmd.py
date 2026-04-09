"""CLI subcommand: summarize a DAG file."""
import argparse
import sys

from pipecheck.formats import DAGLoader, FormatError
from pipecheck.summarizer import DAGSummarizer


def add_summarize_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Register the 'summarize' subcommand."""
    parser = subparsers.add_parser(
        "summarize",
        help="Print a human-readable summary of a DAG file.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output summary as JSON.",
    )
    parser.set_defaults(func=summarize_command)


def summarize_command(args: argparse.Namespace) -> int:
    """Execute the summarize subcommand. Returns exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except (FormatError, FileNotFoundError) as exc:
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    summary = DAGSummarizer(dag).summarize()

    if getattr(args, "json", False):
        import json
        data = {
            "dag_name": summary.dag_name,
            "task_count": summary.task_count,
            "edge_count": summary.edge_count,
            "max_depth": summary.max_depth,
            "root_tasks": summary.root_tasks,
            "leaf_tasks": summary.leaf_tasks,
            "tags_used": sorted(summary.tags_used),
        }
        print(json.dumps(data, indent=2))
    else:
        print(summary)

    return 0
