"""CLI sub-command: pipecheck export — validate a DAG and export the report."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipecheck.formats import DAGLoader
from pipecheck.validator import DAGValidator
from pipecheck.reporter import ValidationReport
from pipecheck.exporter import ReportExporter, ExportError


def add_export_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    """Register the *export* sub-command on *subparsers*."""
    parser = subparsers.add_parser(
        "export",
        help="Validate a DAG definition and export the report.",
    )
    parser.add_argument("file", help="Path to the DAG definition file (JSON/YAML).")
    parser.add_argument(
        "--format",
        "-f",
        choices=["json", "csv", "txt"],
        default="txt",
        dest="fmt",
        help="Output format (default: txt).",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        metavar="PATH",
        help="Write report to this file instead of stdout.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 on warnings as well as errors.",
    )
    parser.set_defaults(func=export_command)


def _resolve_exit_code(report: ValidationReport, strict: bool) -> int:
    """Return the appropriate exit code based on the report and strict flag.

    Returns 1 if the report contains errors, or if *strict* is enabled and
    the report contains warnings.  Returns 0 otherwise.
    """
    if not report.passed:
        return 1
    if strict and report.has_warnings:
        return 1
    return 0


def _print_summary(report: ValidationReport) -> None:
    """Print a brief validation summary to stderr so it is always visible.

    The summary is written to *stderr* to keep *stdout* clean for piped
    output, regardless of whether --output was supplied.
    """
    error_count = len(report.errors)
    warning_count = len(report.warnings)
    status = "PASSED" if report.passed else "FAILED"
    print(
        f"[{status}] {error_count} error(s), {warning_count} warning(s)",
        file=sys.stderr,
    )


def export_command(args: argparse.Namespace) -> int:
    """Execute the export sub-command.  Returns an exit code."""
    try:
        dag = DAGLoader.load_from_file(args.file)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Could not load DAG file: {exc}", file=sys.stderr)
        return 1

    report = ValidationReport(dag_name=dag.name)
    validator = DAGValidator(dag)
    errors, warnings = validator.validate()

    for code, msg, task_id in errors:
        report.add_error(code, msg, task_id=task_id)
    for code, msg, task_id in warnings:
        report.add_warning(code, msg, task_id=task_id)

    exporter = ReportExporter(report)
    try:
        content = exporter.export(args.fmt, dest=args.output)
    except ExportError as exc:
        print(f"[ERROR] Export failed: {exc}", file=sys.stderr)
        return 1

    if args.output is None:
        print(content)

    _print_summary(report)

    return _resolve_exit_code(report, args.strict)
