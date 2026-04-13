"""CLI sub-command: stamp — attach version stamps to DAG tasks."""
from __future__ import annotations

import json
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction

from pipecheck.formats import DAGLoader
from pipecheck.stamper import DAGStamper, StampError


def add_stamp_subparser(subparsers: _SubParsersAction) -> None:
    p: ArgumentParser = subparsers.add_parser(
        "stamp",
        help="Attach version stamps to tasks in a DAG.",
    )
    p.add_argument("dag_file", help="Path to the DAG definition file (JSON/YAML).")
    p.add_argument(
        "--task",
        dest="tasks",
        metavar="ID=VERSION",
        action="append",
        default=[],
        help="Task stamp in ID=VERSION format (repeatable).",
    )
    p.add_argument("--author", default=None, help="Author name to record with stamps.")
    p.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        default=False,
        help="Output results as JSON.",
    )
    p.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if no stamps were applied.",
    )
    p.set_defaults(func=stamp_command)


def stamp_command(args: Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # pragma: no cover
        print(f"[error] Failed to load DAG: {exc}", file=sys.stderr)
        return 1

    versions: dict[str, str] = {}
    for item in args.tasks:
        if "=" not in item:
            print(f"[error] Invalid format '{item}', expected ID=VERSION.", file=sys.stderr)
            return 1
        tid, ver = item.split("=", 1)
        versions[tid.strip()] = ver.strip()

    if not versions:
        print("[warn] No tasks specified for stamping.", file=sys.stderr)
        return 1

    try:
        result = DAGStamper().stamp(dag, versions, author=args.author)
    except StampError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    if args.output_json:
        print(json.dumps([e.to_dict() for e in result.entries], indent=2))
    else:
        print(str(result))

    if args.exit_code and not result.has_stamps:
        return 1
    return 0
