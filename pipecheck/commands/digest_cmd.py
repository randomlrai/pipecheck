"""CLI sub-command: pipecheck digest <dag_file>"""
from __future__ import annotations

import argparse
import sys

from pipecheck.digester import DAGDigester, DigestError
from pipecheck.formats import DAGLoader, FormatError


def add_digest_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "digest",
        help="Compute a fingerprint for a DAG file.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file.")
    parser.add_argument(
        "--algorithm",
        default="sha256",
        metavar="ALGO",
        help="Hash algorithm to use (default: sha256).",
    )
    parser.add_argument(
        "--short",
        action="store_true",
        help="Print only the first 12 characters of the fingerprint.",
    )
    parser.add_argument(
        "--compare",
        metavar="OTHER_FILE",
        default=None,
        help="Compare fingerprint against a second DAG file.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        help="Exit 1 if fingerprints differ when --compare is used.",
    )
    parser.set_defaults(func=digest_command)


def digest_command(args: argparse.Namespace) -> int:
    try:
        dag = DAGLoader.load_from_file(args.dag_file)
    except (FormatError, FileNotFoundError) as exc:
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    try:
        digester = DAGDigester(algorithm=args.algorithm)
        result = digester.digest(dag)
    except DigestError as exc:
        print(f"Digest error: {exc}", file=sys.stderr)
        return 1

    if args.compare:
        try:
            other_dag = DAGLoader.load_from_file(args.compare)
            other_result = digester.digest(other_dag)
        except (FormatError, FileNotFoundError) as exc:
            print(f"Error loading comparison DAG: {exc}", file=sys.stderr)
            return 1

        fp1 = result.short() if args.short else result.fingerprint
        fp2 = other_result.short() if args.short else other_result.fingerprint
        match = result.matches(other_result)
        print(f"{args.dag_file}: {fp1}")
        print(f"{args.compare}: {fp2}")
        print("Match:" + (" yes" if match else " no"))
        if args.exit_code and not match:
            return 1
        return 0

    if args.short:
        print(result.short())
    else:
        print(result)
    return 0
