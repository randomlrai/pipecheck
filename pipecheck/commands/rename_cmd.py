"""CLI sub-command: rename tasks within a DAG definition file."""
from __future__ import annotations
import argparse
import json
import sys
from pipecheck.formats import DAGLoader
from pipecheck.renamer import DAGRenamer, RenameError
from pipecheck.snapshotter import DAGSnapshot


def add_rename_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "rename",
        help="Rename one or more tasks inside a DAG file.",
    )
    parser.add_argument("file", help="Path to the DAG definition file.")
    parser.add_argument(
        "--map",
        metavar="OLD=NEW",
        nargs="+",
        required=True,
        help="One or more old=new task ID pairs.",
    )
    parser.add_argument(
        "--snapshot",
        metavar="PATH",
        default=None,
        help="Save a snapshot of the renamed DAG to this path.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress output.",
    )
    parser.set_defaults(func=rename_command)


def rename_command(args: argparse.Namespace) -> int:
    """Execute the rename sub-command. Returns an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 1

    rename_map: dict[str, str] = {}
    for pair in args.map:
        if "=" not in pair:
            print(f"[error] Invalid map entry '{pair}', expected OLD=NEW", file=sys.stderr)
            return 1
        old, new = pair.split("=", 1)
        rename_map[old.strip()] = new.strip()

    renamer = DAGRenamer()
    try:
        result = renamer.rename(dag, rename_map)
    except RenameError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(str(result))

    if args.snapshot:
        try:
            snap = DAGSnapshot(dag=result.dag)
            snap.save(args.snapshot)
            if not args.quiet:
                print(f"Snapshot saved to {args.snapshot}")
        except Exception as exc:  # noqa: BLE001
            print(f"[warning] Could not save snapshot: {exc}", file=sys.stderr)

    return 0
