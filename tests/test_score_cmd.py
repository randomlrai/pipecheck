from __future__ import annotations

import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.score_cmd import add_score_subparser, score_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(data: dict) -> str:
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    return path


_SIMPLE_DAG = {
    "name": "test_dag",
    "tasks": [
        {"id": "task_a", "description": "First task"},
        {"id": "task_b", "description": "Second task", "depends_on": ["task_a"]},
    ],
}


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = {"exit_code": False, "min_grade": "B", "func": score_command}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddScoreSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_score_subparser(subparsers)
        assert "score" in subparsers.choices

    def test_default_min_grade_is_b(self):
        parser, subparsers = _make_subparsers()
        add_score_subparser(subparsers)
        args = parser.parse_args(["score", "some_file.json"])
        assert args.min_grade == "B"

    def test_exit_code_flag_default_false(self):
        parser, subparsers = _make_subparsers()
        add_score_subparser(subparsers)
        args = parser.parse_args(["score", "some_file.json"])
        assert args.exit_code is False

    def test_exit_code_flag_can_be_set(self):
        parser, subparsers = _make_subparsers()
        add_score_subparser(subparsers)
        args = parser.parse_args(["score", "some_file.json", "--exit-code"])
        assert args.exit_code is True

    def test_min_grade_choices(self):
        parser, subparsers = _make_subparsers()
        add_score_subparser(subparsers)
        args = parser.parse_args(["score", "f.json", "--min-grade", "C"])
        assert args.min_grade == "C"


class TestScoreCommand:
    def setup_method(self):
        self.path = _write_dag(_SIMPLE_DAG)

    def teardown_method(self):
        os.unlink(self.path)

    def test_returns_zero_for_valid_dag(self, capsys):
        args = _make_args(file=self.path)
        result = score_command(args)
        assert result == 0

    def test_output_contains_dag_name(self, capsys):
        args = _make_args(file=self.path)
        score_command(args)
        out = capsys.readouterr().out
        assert "test_dag" in out

    def test_output_contains_grade(self, capsys):
        args = _make_args(file=self.path)
        score_command(args)
        out = capsys.readouterr().out
        assert "Grade" in out

    def test_output_contains_score_line(self, capsys):
        args = _make_args(file=self.path)
        score_command(args)
        out = capsys.readouterr().out
        assert "Score" in out

    def test_exit_code_zero_when_grade_meets_threshold(self, capsys):
        args = _make_args(file=self.path, exit_code=True, min_grade="F")
        result = score_command(args)
        assert result == 0
