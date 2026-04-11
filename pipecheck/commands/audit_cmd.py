"""CLI sub-command: audit — produce a structural audit report for a DAG file."""
import json
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction

from pipecheck.formats import DAGLoader
from pipecheck.auditor import DAGAuditor


def add_audit_subparser(subparsers: "_SubParsersAction") -> None:
    parser: ArgumentParser = subparsers.add_parser(
        "audit",
        help="Produce a structural audit report for a DAG definition file.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON or YAML).")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        dest="output_format",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if the DAG has no root tasks.",
    )
    parser.set_defaults(func=audit_command)


def _print_text_report(report) -> None:
    """Render the audit report in human-readable text format to stdout."""
    print(f"Audit Report — {report.dag_name}")
    print(f"Generated : {report.generated_at}")
    print("-" * 40)
    for entry in report.entries:
        print(entry)


def audit_command(args: Namespace) -> int:
    """Execute the audit sub-command.

    Loads the DAG from *args.file*, runs the auditor, and prints the report in
    the requested format.  Returns a non-zero exit code when ``--exit-code`` is
    set and the DAG contains no root tasks.
    """
    try:
        dag = DAGLoader.load_from_file(args.file)
    except FileNotFoundError:
        print(f"Error loading DAG: file not found: {args.file}", file=sys.stderr)
        return 1
    except Exception as exc:  # pragma: no cover
        print(f"Error loading DAG: {exc}", file=sys.stderr)
        return 1

    auditor = DAGAuditor()
    report = auditor.audit(dag)

    if args.output_format == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        _print_text_report(report)

    if args.exit_code:
        root_entry = next(
            (e for e in report.entries if e.key == "root_tasks"), None
        )
        if root_entry and not root_entry.value:
            return 1

    return 0
