"""CLI subcommand: bounce — apply allow/deny rules to DAG tasks."""
from __future__ import annotations

import argparse
import sys

from pipecheck.bouncer import DAGBouncer
from pipecheck.formats import DAGLoader


def add_bounce_subparser(subparsers) -> None:
    p = subparsers.add_parser(
        "bounce",
        help="Apply allow/deny rules to tasks in a DAG",
    )
    p.add_argument("dag_file", help="Path to the DAG JSON/YAML file")
    p.add_argument(
        "--allow-tag",
        dest="allow_tags",
        metavar="TAG",
        action="append",
        default=[],
        help="Only allow tasks with this tag (repeatable)",
    )
    p.add_argument(
        "--deny-tag",
        dest="deny_tags",
        metavar="TAG",
        action="append",
        default=[],
        help="Deny tasks with this tag (repeatable)",
    )
    p.add_argument(
        "--allow-prefix",
        dest="allow_prefixes",
        metavar="PREFIX",
        action="append",
        default=[],
        help="Only allow tasks whose ID starts with PREFIX (repeatable)",
    )
    p.add_argument(
        "--deny-prefix",
        dest="deny_prefixes",
        metavar="PREFIX",
        action="append",
        default=[],
        help="Deny tasks whose ID starts with PREFIX (repeatable)",
    )
    p.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any tasks are denied",
    )
    p.set_defaults(func=bounce_command)


def bounce_command(args: argparse.Namespace) -> int:
    dag = DAGLoader.load_from_file(args.dag_file)
    bouncer = DAGBouncer(dag)
    result = bouncer.evaluate(
        allow_tags=args.allow_tags or None,
        deny_tags=args.deny_tags or None,
        allow_prefixes=args.allow_prefixes or None,
        deny_prefixes=args.deny_prefixes or None,
    )

    print(str(result))

    if result.has_denials:
        print(f"\n{len(result.denied)} task(s) denied.")
    else:
        print("\nAll tasks allowed.")

    if args.exit_code and result.has_denials:
        return 1
    return 0
