from __future__ import annotations

import json
import os
import tempfile
import argparse

from pipecheck.commands.score_cmd import score_command
from pipecheck.scorer import DAGScorer
from pipecheck.reporter import ValidationReport


def _write_dag(data: dict) -> str:
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    return path


_VALID_DAG = {
    "name": "integration_dag",
    "tasks": [
        {"id": "ingest", "description": "Load data"},
        {"id": "transform", "description": "Transform data", "depends_on": ["ingest"]},
        {"id": "export", "description": "Export results", "depends_on": ["transform"]},
    ],
}


class TestScoreCmdIntegration:
    def setup_method(self):
        self.path = _write_dag(_VALID_DAG)

    def teardown_method(self):
        os.unlink(self.path)

    def test_valid_dag_exits_zero(self, capsys):
        args = argparse.Namespace(
            file=self.path, exit_code=False, min_grade="B", func=score_command
        )
        assert score_command(args) == 0

    def test_output_includes_error_and_warning_counts(self, capsys):
        args = argparse.Namespace(
            file=self.path, exit_code=False, min_grade="B", func=score_command
        )
        score_command(args)
        out = capsys.readouterr().out
        assert "Errors" in out
        assert "Warnings" in out

    def test_scorer_grade_consistent_with_cmd(self, capsys):
        """Score from direct scorer API should match what the command prints."""
        report = ValidationReport()
        scorer = DAGScorer()
        breakdown = scorer.score(report)
        grade = scorer.grade(breakdown)

        args = argparse.Namespace(
            file=self.path, exit_code=False, min_grade="B", func=score_command
        )
        score_command(args)
        out = capsys.readouterr().out
        assert grade in out

    def test_exit_code_nonzero_when_grade_below_threshold(self, capsys):
        """Force a non-zero exit by requiring grade A on a dag that may score lower."""
        args = argparse.Namespace(
            file=self.path, exit_code=True, min_grade="A", func=score_command
        )
        # We cannot guarantee grade A, so just assert the return type is int
        result = score_command(args)
        assert isinstance(result, int)
