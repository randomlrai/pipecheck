"""CLI sub-command: pipecheck tag — list or filter tasks by tag."""
from __future__ import annotations
import argparse
import sys
from pipecheck.formats import DAGLoader
from pipecheck.tagger import DAGTagger


def add_tag_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser("tag", help="List or filter DAG tasks by tag")
    parser.add_argument("file", help="Path to DAG definition file (JSON/YAML)")
    parser.add_argument(
        "--filter",
        metavar="TAG",
        dest="filter_tag",
        default=None,
        help="Show only tasks with this tag",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        default=False,
        help="Print tag -> task-count summary",
    )
    parser.set_defaults(func=tag_command)


def tag_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    tagger = DAGTagger(dag)

    if args.summary:
        summary = tagger.summary()
        if not summary:
            print("No tags found.")
        else:
            for tag, count in summary.items():
                print(f"{tag}: {count} task(s)")
        return 0

    if args.filter_tag:
        task_ids = tagger.filter_dag(args.filter_tag)
        if not task_ids:
            print(f"No tasks found with tag '{args.filter_tag}'.")
            return 0
        for tid in task_ids:
            print(tid)
        return 0

    # Default: print all tags
    tags = tagger.index.all_tags()
    if not tags:
        print("No tags found.")
    else:
        for tag in tags:
            print(tag)
    return 0
