"""CLI subcommand: feed — apply external metadata to DAG tasks."""
from __future__ import annotations
import json
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pipecheck.formats import DAGLoader
from pipecheck.feeder import DAGFeeder, FeedError


def add_feed_subparser(subparsers: _SubParsersAction) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "feed", help="Apply external metadata fields to DAG tasks"
    )
    p.add_argument("dag_file", help="Path to DAG JSON/YAML file")
    p.add_argument("feed_file", help="Path to JSON file mapping task_id -> fields")
    p.add_argument("--source", default="external", help="Label for the data source")
    p.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit 1 if no feeds were applied",
    )
    p.set_defaults(func=feed_command)


def feed_command(args: Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        with open(args.feed_file) as fh:
            data = json.load(fh)
    except Exception as exc:
        print(f"Error loading feed file: {exc}", file=sys.stderr)
        return 1

    feeder = DAGFeeder(dag)
    try:
        result = feeder.feed(args.source, data)
    except FeedError as exc:
        print(f"Feed error: {exc}", file=sys.stderr)
        return 1

    print(str(result))

    if args.exit_code and not result.has_feeds():
        return 1
    return 0
