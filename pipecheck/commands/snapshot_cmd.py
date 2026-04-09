"""CLI sub-commands: ``pipecheck snapshot save`` and ``pipecheck snapshot load``."""

from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.snapshotter import DAGSnapshot, SnapshotError


def add_snapshot_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "snapshot",
        help="Save or load a DAG snapshot.",
    )
    sub = parser.add_subparsers(dest="snapshot_action", required=True)

    # ---- save ----
    save_p = sub.add_parser("save", help="Save current DAG state to a snapshot file.")
    save_p.add_argument("dag_file", help="Path to the DAG definition file.")
    save_p.add_argument("output", help="Destination snapshot file (.json).")
    save_p.add_argument("--label", default=None, help="Human-readable label for the snapshot.")

    # ---- load ----
    load_p = sub.add_parser("load", help="Display information from a saved snapshot.")
    load_p.add_argument("snapshot_file", help="Path to the snapshot file.")


def snapshot_command(args: argparse.Namespace) -> int:
    """Entry point for the *snapshot* command. Returns an exit code."""
    action = getattr(args, "snapshot_action", None)

    if action == "save":
        try:
            dag = DAGLoader.load_from_file(args.dag_file)
        except Exception as exc:  # noqa: BLE001
            print(f"[error] Failed to load DAG: {exc}", file=sys.stderr)
            return 1

        snap = DAGSnapshot(dag, label=args.label)
        try:
            snap.save(args.output)
        except SnapshotError as exc:
            print(f"[error] {exc}", file=sys.stderr)
            return 1

        print(f"Snapshot saved to '{args.output}' (label={snap.label}, tasks={len(dag.tasks)}).")
        return 0

    if action == "load":
        try:
            snap = DAGSnapshot.load(args.snapshot_file)
        except SnapshotError as exc:
            print(f"[error] {exc}", file=sys.stderr)
            return 1

        dag = snap.dag
        print(f"Snapshot: {snap.label}")
        print(f"  Created : {snap.created_at}")
        print(f"  DAG     : {dag.name}")
        print(f"  Tasks   : {len(dag.tasks)}")
        for task_id in sorted(dag.tasks):
            print(f"    - {task_id}")
        return 0

    print("[error] Unknown snapshot action.", file=sys.stderr)
    return 1
