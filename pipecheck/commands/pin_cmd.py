"""CLI sub-command: pin — capture or compare pinned DAG configurations."""
from __future__ import annotations

import json
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction

from pipecheck.formats import DAGLoader
from pipecheck.pinner import DAGPin, DAGPinner


def add_pin_subparser(subparsers: _SubParsersAction) -> None:
    parser: ArgumentParser = subparsers.add_parser(
        "pin",
        help="Pin the current DAG configuration or diff two pins.",
    )
    parser.add_argument("dag_file", help="Path to the DAG definition file.")
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        metavar="FILE",
        help="Write the pin as JSON to FILE (default: stdout).",
    )
    parser.add_argument(
        "--compare",
        "-c",
        default=None,
        metavar="PIN_FILE",
        help="Compare current DAG against a previously saved pin (JSON).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 when differences are found (requires --compare).",
    )
    parser.set_defaults(func=pin_command)


def pin_command(args: Namespace) -> int:
    dag = DAGLoader.load_from_file(args.dag_file)
    pinner = DAGPinner()
    current_pin = pinner.pin(dag)

    if args.compare:
        with open(args.compare, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        old_pin = _pin_from_dict(raw)
        changes = pinner.diff_pins(old_pin, current_pin)
        if changes:
            print(f"Pin diff ({len(changes)} change(s)):")
            for line in changes:
                print(f"  - {line}")
            if args.exit_code:
                return 1
        else:
            print("No changes detected.")
        return 0

    pin_json = json.dumps(current_pin.to_dict(), indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(pin_json)
        print(f"Pin written to {args.output}")
    else:
        print(pin_json)
    return 0


def _pin_from_dict(data: dict) -> DAGPin:
    from pipecheck.pinner import PinnedTask

    pin = DAGPin(dag_name=data["dag_name"])
    for tid, attrs in data.get("tasks", {}).items():
        pin.tasks[tid] = PinnedTask(
            task_id=tid,
            timeout=attrs.get("timeout"),
            retries=int(attrs.get("retries", 0)),
            tags=list(attrs.get("tags", [])),
        )
    return pin
