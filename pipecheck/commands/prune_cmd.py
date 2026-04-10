"""CLI sub-command: prune — remove unreachable tasks from a DAG."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.pruner import DAGPruner, PruneError
from pipecheck.snapshotter import DAGSnapshot


def add_prune_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    """Register the *prune* sub-command."""
    parser = subparsers.add_parser(
        "prune",
        help="Remove tasks unreachable from the given root tasks.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "roots",
        nargs="+",
        metavar="TASK_ID",
        help="One or more root task IDs to keep (along with their dependencies).",
    )
    parser.add_argument(
        "--save-snapshot",
        metavar="PATH",
        default=None,
        help="If provided, save a snapshot of the pruned DAG to this path.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when any tasks were removed.",
    )
    parser.set_defaults(func=prune_command)


def prune_command(args: argparse.Namespace) -> int:
    """Execute the prune sub-command."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 1

    pruner = DAGPruner(dag)
    try:
        result = pruner.prune(args.roots)
    except PruneError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    print(result)

    if args.save_snapshot:
        try:
            snapshot = DAGSnapshot(result.dag)
            snapshot.save(args.save_snapshot)
            print(f"Snapshot saved to {args.save_snapshot}")
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] Could not save snapshot: {exc}", file=sys.stderr)

    if args.exit_code and result.has_removals:
        return 1
    return 0
