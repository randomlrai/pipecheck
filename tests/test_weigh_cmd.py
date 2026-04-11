"""Tests for pipecheck.commands.weigh_cmd."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.weigh_cmd import add_weigh_subparser, weigh_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(tmp_path: str, tasks=None) -> str:
    if tasks is None:
        tasks = [
            {"task_id": "a", "command": "echo a", "metadata": {"weight": 2.0}},
            {"task_id": "b", "command": "echo b", "dependencies": ["a"], "metadata": {"weight": 3.0}},
        ]
    data = {"name": "test_dag", "tasks": tasks}
    path = os.path.join(tmp_path, "dag.json")
    with open(path, "w") as fh:
        json.dump(data, fh)
    return path


def _make_args(**kwargs):
    defaults = {"file": "dag.json", "weight_key": "weight", "exit_code": False, "func": weigh_command}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddWeighSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_weigh_subparser(subparsers)
        assert "weigh" in subparsers.choices

    def test_default_weight_key(self):
        _, subparsers = _make_subparsers()
        add_weigh_subparser(subparsers)
        ns = subparsers.choices["weigh"].parse_args(["somefile.json"])
        assert ns.weight_key == "weight"

    def test_custom_weight_key(self):
        _, subparsers = _make_subparsers()
        add_weigh_subparser(subparsers)
        ns = subparsers.choices["weigh"].parse_args(["f.json", "--weight-key", "cost"])
        assert ns.weight_key == "cost"

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_weigh_subparser(subparsers)
        ns = subparsers.choices["weigh"].parse_args(["f.json"])
        assert ns.exit_code is False

    def test_exit_code_flag_set(self):
        _, subparsers = _make_subparsers()
        add_weigh_subparser(subparsers)
        ns = subparsers.choices["weigh"].parse_args(["f.json", "--exit-code"])
        assert ns.exit_code is True


class TestWeighCommand:
    def test_returns_zero_on_valid_dag(self, tmp_path):
        path = _write_dag(str(tmp_path))
        args = _make_args(file=path)
        assert weigh_command(args) == 0

    def test_returns_one_on_missing_file(self, tmp_path):
        args = _make_args(file=str(tmp_path / "missing.json"))
        assert weigh_command(args) == 1

    def test_returns_zero_empty_dag_no_exit_code(self, tmp_path):
        path = _write_dag(str(tmp_path), tasks=[])
        args = _make_args(file=path, exit_code=False)
        assert weigh_command(args) == 0

    def test_returns_one_empty_dag_with_exit_code(self, tmp_path):
        path = _write_dag(str(tmp_path), tasks=[])
        args = _make_args(file=path, exit_code=True)
        assert weigh_command(args) == 1

    def test_warns_when_no_weight_metadata(self, tmp_path, capsys):
        tasks = [{"task_id": "a", "command": "echo"}]
        path = _write_dag(str(tmp_path), tasks=tasks)
        args = _make_args(file=path, exit_code=False)
        weigh_command(args)
        out = capsys.readouterr().out
        assert "[warn]" in out

    def test_exit_code_one_when_no_weight_metadata_and_flag_set(self, tmp_path):
        tasks = [{"task_id": "a", "command": "echo"}]
        path = _write_dag(str(tmp_path), tasks=tasks)
        args = _make_args(file=path, exit_code=True)
        assert weigh_command(args) == 1
