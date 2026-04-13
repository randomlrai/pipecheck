"""CLI sub-command: link — validate cross-DAG task references."""

from __future__ import annotations

import argparse
import json
import sys

from pipecheck.formats import DAGLoader
from pipecheck.linker import DAGLinker, LinkEntry


def add_link_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "link",
        help="Validate cross-DAG task references from a link spec file.",
    )
    parser.add_argument(
        "spec",
        help="JSON file containing a list of link definitions.",
    )
    parser.add_argument(
        "dags",
        nargs="+",
        help="One or more DAG definition files to register.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any links are unresolved.",
    )
    parser.set_defaults(func=link_command)


def _load_spec(spec_path: str) -> list[dict]:
    """Load and parse the link spec JSON file.

    Raises SystemExit with a descriptive message if the file is missing,
    not valid JSON, or does not contain a top-level list.
    """
    try:
        with open(spec_path) as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        print(f"error: spec file not found: {spec_path}", file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError as exc:
        print(f"error: invalid JSON in spec file: {exc}", file=sys.stderr)
        sys.exit(2)

    if not isinstance(raw, list):
        print(
            f"error: spec file must contain a JSON array, got {type(raw).__name__}",
            file=sys.stderr,
        )
        sys.exit(2)

    return raw


def link_command(args: argparse.Namespace) -> None:
    linker = DAGLinker()

    for dag_path in args.dags:
        dag = DAGLoader.load_from_file(dag_path)
        linker.register(dag)

    raw = _load_spec(args.spec)

    entries = [
        LinkEntry(
            source_dag=item["source_dag"],
            source_task=item["source_task"],
            target_dag=item["target_dag"],
            target_task=item["target_task"],
        )
        for item in raw
    ]

    result = linker.link(entries)
    print(result)

    if args.exit_code and result.has_unresolved:
        sys.exit(1)
