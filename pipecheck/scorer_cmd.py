from __future__ import annotations

import argparse

from pipecheck.formats import DAGLoader
from pipecheck.validator import DAGValidator
from pipecheck.reporter import ValidationReport, Severity
from pipecheck.scorer import DAGScorer


def add_score_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "score",
        help="Score a DAG and return a quality grade",
    )
    parser.add_argument("file", help="Path to the DAG file (JSON or YAML)")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with non-zero code when grade is below B",
    )
    parser.add_argument(
        "--min-grade",
        default="B",
        choices=["A", "B", "C", "D", "F"],
        help="Minimum acceptable grade (default: B)",
    )
    parser.set_defaults(func=score_command)


def score_command(args: argparse.Namespace) -> int:
    dag = DAGLoader.load_from_file(args.file)
    validator = DAGValidator(dag)
    report = ValidationReport()

    errors = validator.validate()
    for err in errors:
        if err.is_warning:
            report.add_warning(err.message)
        else:
            report.add_error(err.message)

    scorer = DAGScorer()
    breakdown = scorer.score(report)
    grade = scorer.grade(breakdown)

    print(f"DAG : {dag.name}")
    print(f"Score : {breakdown.score:.1f} / {breakdown.max_score:.1f}")
    print(f"Grade : {grade}")
    print(f"Errors : {breakdown.error_count}")
    print(f"Warnings: {breakdown.warning_count}")

    grade_order = ["A", "B", "C", "D", "F"]
    if args.exit_code and grade_order.index(grade) > grade_order.index(args.min_grade):
        return 1
    return 0
