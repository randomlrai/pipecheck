"""CLI sub-command: sample — randomly select tasks from a DAG file."""
from __future__ import annotations

import argparse
import sys

from pipecheck.formats import DAGLoader
from pipecheck.sampler import DAGSampler, SampleError


def add_sample_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "sample",
        help="Randomly sample N tasks from a DAG.",
    )
    parser.add_argument("file", help="Path to DAG file (JSON or YAML).")
    parser.add_argument(
        "n",
        type=int,
        help="Number of tasks to sample.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when the sample is empty.",
    )
    parser.set_defaults(func=sample_command)


def sample_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    sampler = DAGSampler()
    try:
        result = sampler.sample(dag, args.n, seed=args.seed)
    except SampleError as exc:
        print(f"Sample error: {exc}", file=sys.stderr)
        return 1

    print(result)

    if args.exit_code and not result.has_sample:
        return 1
    return 0
