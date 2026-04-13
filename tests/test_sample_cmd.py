"""Tests for pipecheck.commands.sample_cmd."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.sample_cmd import add_sample_subparser, sample_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str, n_tasks: int = 5) -> None:
    tasks = [
        {"task_id": f"task_{i}", "command": f"run_{i}.sh"}
        for i in range(n_tasks)
    ]
    data = {"name": "test_dag", "tasks": tasks, "edges": []}
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "n": 2,
        "seed": None,
        "exit_code": False,
        "func": sample_command,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddSampleSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_sample_subparser(subparsers)
        assert "sample" in subparsers.choices

    def test_default_seed_is_none(self):
        _, subparsers = _make_subparsers()
        add_sample_subparser(subparsers)
        ns = subparsers.choices["sample"].parse_args(["dag.json", "3"])
        assert ns.seed is None

    def test_seed_flag_parsed(self):
        _, subparsers = _make_subparsers()
        add_sample_subparser(subparsers)
        ns = subparsers.choices["sample"].parse_args(["dag.json", "2", "--seed", "42"])
        assert ns.seed == 42

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_sample_subparser(subparsers)
        ns = subparsers.choices["sample"].parse_args(["dag.json", "1"])
        assert ns.exit_code is False

    def test_exit_code_flag_set(self):
        _, subparsers = _make_subparsers()
        add_sample_subparser(subparsers)
        ns = subparsers.choices["sample"].parse_args(["dag.json", "1", "--exit-code"])
        assert ns.exit_code is True


class TestSampleCommand:
    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self._tmp.close()
        _write_dag(self._tmp.name, n_tasks=5)

    def teardown_method(self):
        os.unlink(self._tmp.name)

    def test_returns_zero_on_success(self):
        args = _make_args(file=self._tmp.name, n=3)
        assert sample_command(args) == 0

    def test_returns_zero_when_n_is_zero(self):
        args = _make_args(file=self._tmp.name, n=0)
        assert sample_command(args) == 0

    def test_exit_code_flag_triggers_nonzero_on_empty(self):
        args = _make_args(file=self._tmp.name, n=0, exit_code=True)
        assert sample_command(args) == 1

    def test_exit_code_flag_zero_when_sample_nonempty(self):
        args = _make_args(file=self._tmp.name, n=2, exit_code=True)
        assert sample_command(args) == 0

    def test_returns_one_on_bad_file(self):
        args = _make_args(file="/nonexistent/dag.json", n=1)
        assert sample_command(args) == 1

    def test_returns_one_on_n_too_large(self):
        args = _make_args(file=self._tmp.name, n=100)
        assert sample_command(args) == 1

    def test_seed_produces_deterministic_output(self, capsys):
        args1 = _make_args(file=self._tmp.name, n=3, seed=7)
        args2 = _make_args(file=self._tmp.name, n=3, seed=7)
        sample_command(args1)
        out1 = capsys.readouterr().out
        sample_command(args2)
        out2 = capsys.readouterr().out
        assert out1 == out2
