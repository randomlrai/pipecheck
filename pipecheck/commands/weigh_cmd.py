"""CLI sub-command: weigh — display task weights for a DAG file."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.weigher import DAGWeigher


def add_weigh_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "weigh",
        help="Analyse task weights and identify the heaviest path in a DAG.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--weight-key",
        default="weight",
        metavar="KEY",
        help="Metadata key used as the task weight (default: weight).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when the DAG has no weight metadata at all.",
    )
    parser.set_defaults(func=weigh_command)


def _warn_no_custom_weights(weight_key: str) -> None:
    """Print a warning when no tasks carry custom weight metadata."""
    print(
        f"[warn] No tasks have '{weight_key}' metadata; "
        "default weight of 1.0 used for all tasks."
    )


def weigh_command(args: argparse.Namespace) -> int:
    """Entry point for the ``weigh`` sub-command.

    Loads a DAG from *args.file*, computes per-task cumulative weights using
    *args.weight_key* as the metadata key, and prints a summary table together
    with the heaviest task.  Returns a non-zero exit code when ``--exit-code``
    is set and the DAG contains no custom weight metadata.
    """
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Could not load DAG: {exc}", file=sys.stderr)
        return 1

    weigher = DAGWeigher(weight_key=args.weight_key)
    result = weigher.weigh(dag)

    if not result.has_weights():
        print("[warn] DAG contains no tasks — nothing to weigh.")
        return 1 if args.exit_code else 0

    print(str(result))

    heaviest = result.heaviest_task
    if heaviest:
        print(f"\nHeaviest task: {heaviest.task_id} (cumulative={heaviest.cumulative_weight:.2f})")

    any_custom = any(
        args.weight_key in t.metadata for t in dag.tasks.values()
    )
    if not any_custom:
        _warn_no_custom_weights(args.weight_key)
        if args.exit_code:
            return 1

    return 0
