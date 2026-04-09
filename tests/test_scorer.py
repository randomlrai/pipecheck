"""Tests for pipecheck.scorer."""

import pytest

from pipecheck.scorer import DAGScorer, ScoreBreakdown, MAX_SCORE
from pipecheck.reporter import ValidationReport, Severity


def _make_report(*severities: Severity) -> ValidationReport:
    report = ValidationReport(dag_name="test_dag")
    for sev in severities:
        if sev == Severity.ERROR:
            report.add_error("some error")
        elif sev == Severity.WARNING:
            report.add_warning("some warning")
        else:
            report.add_info("some info")
    return report


class TestDAGScorer:
    def setup_method(self):
        self.scorer = DAGScorer()

    # --- score() ---

    def test_empty_report_gives_max_score(self):
        report = _make_report()
        bd = self.scorer.score(report)
        assert bd.final_score == MAX_SCORE
        assert bd.penalties == []

    def test_single_error_deducts_25(self):
        report = _make_report(Severity.ERROR)
        bd = self.scorer.score(report)
        assert bd.final_score == MAX_SCORE - 25

    def test_single_warning_deducts_5(self):
        report = _make_report(Severity.WARNING)
        bd = self.scorer.score(report)
        assert bd.final_score == MAX_SCORE - 5

    def test_single_info_deducts_1(self):
        report = _make_report(Severity.INFO)
        bd = self.scorer.score(report)
        assert bd.final_score == MAX_SCORE - 1

    def test_mixed_issues_accumulate_penalties(self):
        report = _make_report(Severity.ERROR, Severity.WARNING, Severity.INFO)
        bd = self.scorer.score(report)
        assert bd.final_score == MAX_SCORE - 25 - 5 - 1

    def test_score_never_goes_below_zero(self):
        report = _make_report(*([Severity.ERROR] * 10))
        bd = self.scorer.score(report)
        assert bd.final_score == 0

    def test_penalties_list_has_one_entry_per_issue(self):
        report = _make_report(Severity.ERROR, Severity.WARNING)
        bd = self.scorer.score(report)
        assert len(bd.penalties) == 2

    def test_penalty_entry_contains_severity_label(self):
        report = _make_report(Severity.ERROR)
        bd = self.scorer.score(report)
        assert "ERROR" in bd.penalties[0]

    # --- grade() ---

    def test_grade_A(self):
        assert self.scorer.grade(95) == "A"

    def test_grade_B(self):
        assert self.scorer.grade(80) == "B"

    def test_grade_C(self):
        assert self.scorer.grade(65) == "C"

    def test_grade_D(self):
        assert self.scorer.grade(50) == "D"

    def test_grade_F(self):
        assert self.scorer.grade(30) == "F"

    def test_grade_boundary_90_is_A(self):
        assert self.scorer.grade(90) == "A"

    def test_grade_boundary_75_is_B(self):
        assert self.scorer.grade(75) == "B"
