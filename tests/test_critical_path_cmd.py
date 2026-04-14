"""Tests for pipecheck.commands.critical_path_cmd."""
import argparse
import json
import os
import tempfile
from unittest.mock import patch

import pytest

from pipecheck.commands.critical_path_cmd import (
    add_critical_path_subparser,
    critical_path_command,
)


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(data: dict) -> str:
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f)
    return path


def _make_args(**kwargs):
    defaults = dict(file="dag.json", weight="timeout", exit_code=False, threshold=None)
    defaults.update(kwargs)
    ns = argparse.Namespace(**defaults)
    ns.func = critical_path_command
    return ns


_SIMPLE_DAG = {
    "name": "pipe",
    "tasks": [
        {"id": "a", "metadata": {"timeout": 2}},
        {"id": "b", "metadata": {"timeout": 5}},
    ],
    "edges": [["a", "b"]],
}


class TestAddCriticalPathSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_critical_path_subparser(subs)
        parser, _ = _make_subparsers()
        add_critical_path_subparser(parser.add_subparsers(dest="command"))
        args = parser.parse_args(["critical-path", "some.json"])
        assert args.command == "critical-path"

    def test_default_weight_is_timeout(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_critical_path_subparser(subs)
        args = parser.parse_args(["critical-path", "dag.json"])
        assert args.weight == "timeout"

    def test_weight_flag_overrides(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_critical_path_subparser(subs)
        args = parser.parse_args(["critical-path", "dag.json", "--weight", "cost"])
        assert args.weight == "cost"

    def test_exit_code_flag_default_false(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_critical_path_subparser(subs)
        args = parser.parse_args(["critical-path", "dag.json"])
        assert args.exit_code is False


class TestCriticalPathCommand:
    def test_returns_zero_on_valid_dag(self):
        path = _write_dag(_SIMPLE_DAG)
        try:
            code = critical_path_command(_make_args(file=path))
            assert code == 0
        finally:
            os.unlink(path)

    def test_returns_one_on_missing_file(self, capsys):
        code = critical_path_command(_make_args(file="/nonexistent/dag.json"))
        assert code == 1
        captured = capsys.readouterr()
        assert "error" in captured.err.lower()

    def test_output_contains_dag_name(self, capsys):
        path = _write_dag(_SIMPLE_DAG)
        try:
            critical_path_command(_make_args(file=path))
            captured = capsys.readouterr()
            assert "pipe" in captured.out
        finally:
            os.unlink(path)

    def test_threshold_exceeded_returns_one(self, capsys):
        path = _write_dag(_SIMPLE_DAG)
        try:
            code = critical_path_command(
                _make_args(file=path, exit_code=True, threshold=1.0)
            )
            assert code == 1
        finally:
            os.unlink(path)

    def test_threshold_not_exceeded_returns_zero(self, capsys):
        path = _write_dag(_SIMPLE_DAG)
        try:
            code = critical_path_command(
                _make_args(file=path, exit_code=True, threshold=100.0)
            )
            assert code == 0
        finally:
            os.unlink(path)
