"""Tests for pipecheck.commands.optimize_cmd."""
import argparse
import json
import os
import tempfile
import pytest
from unittest.mock import patch
from pipecheck.commands.optimize_cmd import add_optimize_subparser, optimize_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str, tasks=None) -> None:
    tasks = tasks or [
        {"task_id": "a"},
        {"task_id": "b", "dependencies": ["a"]},
        {"task_id": "c", "dependencies": ["b"]},
    ]
    payload = {"name": "test_dag", "tasks": tasks}
    with open(path, "w") as f:
        json.dump(payload, f)


def _make_args(dag_file: str, exit_code: bool = False) -> argparse.Namespace:
    return argparse.Namespace(dag_file=dag_file, exit_code=exit_code)


class TestAddOptimizeSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_optimize_subparser(subparsers)
        assert "optimize" in subparsers.choices

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_optimize_subparser(subparsers)
        args = subparsers.choices["optimize"].parse_args(["dag.json"])
        assert args.exit_code is False

    def test_exit_code_flag_can_be_set(self):
        _, subparsers = _make_subparsers()
        add_optimize_subparser(subparsers)
        args = subparsers.choices["optimize"].parse_args(["dag.json", "--exit-code"])
        assert args.exit_code is True

    def test_func_set_to_optimize_command(self):
        _, subparsers = _make_subparsers()
        add_optimize_subparser(subparsers)
        args = subparsers.choices["optimize"].parse_args(["dag.json"])
        assert args.func is optimize_command


class TestOptimizeCommand:
    def test_returns_zero_on_clean_dag(self, tmp_path):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(
            dag_file,
            tasks=[
                {"task_id": "a"},
                {"task_id": "b"},
                {"task_id": "c", "dependencies": ["a", "b"]},
            ],
        )
        args = _make_args(dag_file)
        assert optimize_command(args) == 0

    def test_returns_zero_without_exit_code_flag_even_with_hints(self, tmp_path):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(dag_file)  # linear chain -> parallelism hint
        args = _make_args(dag_file, exit_code=False)
        assert optimize_command(args) == 0

    def test_returns_one_with_exit_code_flag_and_hints(self, tmp_path):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(dag_file)  # linear chain -> parallelism hint
        args = _make_args(dag_file, exit_code=True)
        assert optimize_command(args) == 1

    def test_returns_one_on_bad_file(self, tmp_path):
        args = _make_args(str(tmp_path / "missing.json"))
        assert optimize_command(args) == 1

    def test_output_contains_dag_name(self, tmp_path, capsys):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(dag_file)
        optimize_command(_make_args(dag_file))
        captured = capsys.readouterr()
        assert "test_dag" in captured.out
