from __future__ import annotations

import argparse

from pipecheck.formats import DAGLoader
from pipecheck.validator import DAGValidator
from pipecheck.reporter import ValidationReport
from pipecheck.scorer import DAGScorer


def add_score_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Register the 'score' subcommand."""
    parser = subparsers.add_parser(
        "score",
        help="Score a DAG and return a quality grade",
    )
    parser.add_argument("file", help="Path to the DAG file (JSON or YAML)")
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with non-zero code when grade is below --min-grade",
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

    for err in validator.validate():
        if getattr(err, "is_warning", False):
            report.add_warning(err.message)
        else:
            report.add_error(err.message)

    scorer = DAGScorer()
    breakdown = scorer.score(report)
    grade = scorer.grade(breakdown)

    print(f"DAG     : {dag.name}")
    print(f"Score   : {breakdown.score:.1f} / {breakdown.max_score:.1f}")
    print(f"Grade   : {grade}")
    print(f"Errors  : {breakdown.error_count}")
    print(f"Warnings: {breakdown.warning_count}")

    _grade_rank = {"A": 0, "B": 1, "C": 2, "D": 3, "F": 4}
    if args.exit_code and _grade_rank[grade] > _grade_rank[args.min_grade]:
        return 1
    return 0
