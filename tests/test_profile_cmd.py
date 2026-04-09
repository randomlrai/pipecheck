"""Tests for pipecheck.commands.profile_cmd."""

import argparse
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from pipecheck.commands.profile_cmd import add_profile_subparser, profile_command


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "bottlenecks_only": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str) -> None:
    dag_data = {
        "dag_id": "cmd_test_dag",
        "tasks": [
            {"task_id": "ingest", "dependencies": []},
            {"task_id": "transform", "dependencies": ["ingest"]},
            {"task_id": "load_a", "dependencies": ["transform"]},
            {"task_id": "load_b", "dependencies": ["transform"]},
        ],
    }
    with open(path, "w") as fh:
        json.dump(dag_data, fh)


class TestAddProfileSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_profile_subparser(subparsers)
        # Should not raise

    def test_default_flags(self):
        parser, subparsers = _make_subparsers()
        add_profile_subparser(subparsers)
        args = parser.parse_args(["profile", "some.json"])
        assert args.bottlenecks_only is False
        assert args.exit_code is False

    def test_bottlenecks_only_flag(self):
        parser, subparsers = _make_subparsers()
        add_profile_subparser(subparsers)
        args = parser.parse_args(["profile", "dag.json", "--bottlenecks-only"])
        assert args.bottlenecks_only is True

    def test_exit_code_flag(self):
        parser, subparsers = _make_subparsers()
        add_profile_subparser(subparsers)
        args = parser.parse_args(["profile", "dag.json", "--exit-code"])
        assert args.exit_code is True

    def test_func_set(self):
        parser, subparsers = _make_subparsers()
        add_profile_subparser(subparsers)
        args = parser.parse_args(["profile", "dag.json"])
        assert args.func is profile_command


class TestProfileCommand:
    def test_returns_zero_on_success(self, tmp_path):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(dag_file)
        args = _make_args(file=dag_file)
        assert profile_command(args) == 0

    def test_returns_one_on_missing_file(self):
        args = _make_args(file="/nonexistent/dag.json")
        assert profile_command(args) == 1

    def test_summary_printed(self, tmp_path, capsys):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(dag_file)
        args = _make_args(file=dag_file)
        profile_command(args)
        captured = capsys.readouterr()
        assert "Tasks" in captured.out

    def test_bottlenecks_only_output(self, tmp_path, capsys):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(dag_file)
        args = _make_args(file=dag_file, bottlenecks_only=True)
        profile_command(args)
        captured = capsys.readouterr()
        assert "transform" in captured.out
        assert "Tasks" not in captured.out

    def test_exit_code_with_bottlenecks(self, tmp_path):
        dag_file = str(tmp_path / "dag.json")
        _write_dag(dag_file)
        args = _make_args(file=dag_file, exit_code=True)
        assert profile_command(args) == 1

    def test_exit_code_without_bottlenecks(self, tmp_path):
        dag_file = str(tmp_path / "dag.json")
        # Single task, no bottleneck
        data = {"dag_id": "solo", "tasks": [{"task_id": "only", "dependencies": []}]}
        with open(dag_file, "w") as fh:
            json.dump(data, fh)
        args = _make_args(file=dag_file, exit_code=True)
        assert profile_command(args) == 0
