"""CLI subcommand: pipecheck patch — apply field patches to DAG tasks."""
from __future__ import annotations
import json
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pipecheck.formats import DAGLoader
from pipecheck.patcher import DAGPatcher, PatchError


def add_patch_subparser(subparsers: _SubParsersAction) -> None:
    parser: ArgumentParser = subparsers.add_parser(
        "patch",
        help="Apply field-level updates to tasks in a DAG.",
    )
    parser.add_argument("dag_file", help="Path to the DAG file (JSON or YAML).")
    parser.add_argument(
        "--spec",
        required=True,
        help=(
            'JSON string mapping task_id -> {field: value}. '
            'Example: \'{"ingest": {"timeout": 120}}\'
        ),
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if no patches were applied.",
    )
    parser.set_defaults(func=patch_command)


def patch_command(args: Namespace) -> int:
    try:
        spec = json.loads(args.spec)
    except json.JSONDecodeError as exc:
        print(f"[error] Invalid JSON spec: {exc}", file=sys.stderr)
        return 1

    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Failed to load DAG: {exc}", file=sys.stderr)
        return 1

    patcher = DAGPatcher()
    try:
        result = patcher.patch(dag, spec)
    except PatchError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    print(str(result))

    if args.exit_code and not result.has_patches:
        return 1
    return 0
