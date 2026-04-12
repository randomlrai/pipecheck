"""Tests for pipecheck.commands.depend_cmd."""
import argparse
import json
import os
import tempfile
import pytest
from pipecheck.commands.depend_cmd import add_depend_subparser, depend_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str) -> None:
    data = {
        "name": "mypipe",
        "tasks": [
            {"task_id": "a"},
            {"task_id": "b"},
            {"task_id": "c"},
        ],
        "edges": [["a", "b"], ["b", "c"]],
    }
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "task": "b",
        "ancestors_only": False,
        "descendants_only": False,
        "exit_code": False,
        "func": depend_command,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddDependSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_depend_subparser(subparsers)
        assert "depend" in subparsers.choices

    def test_default_flags_are_false(self):
        _, subparsers = _make_subparsers()
        add_depend_subparser(subparsers)
        args = subparsers.choices["depend"].parse_args(["dag.json", "t"])
        assert args.ancestors_only is False
        assert args.descendants_only is False
        assert args.exit_code is False

    def test_ancestors_only_flag(self):
        _, subparsers = _make_subparsers()
        add_depend_subparser(subparsers)
        args = subparsers.choices["depend"].parse_args(["dag.json", "t", "--ancestors-only"])
        assert args.ancestors_only is True

    def test_descendants_only_flag(self):
        _, subparsers = _make_subparsers()
        add_depend_subparser(subparsers)
        args = subparsers.choices["depend"].parse_args(["dag.json", "t", "--descendants-only"])
        assert args.descendants_only is True


class TestDependCommand:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        _write_dag(self.tmp.name)
        self.tmp.close()

    def teardown_method(self):
        os.unlink(self.tmp.name)

    def test_valid_task_exits_zero(self):
        args = _make_args(file=self.tmp.name, task="b")
        assert depend_command(args) == 0

    def test_unknown_task_exits_one(self):
        args = _make_args(file=self.tmp.name, task="zzz")
        assert depend_command(args) == 1

    def test_bad_file_exits_one(self):
        args = _make_args(file="/nonexistent/dag.json", task="a")
        assert depend_command(args) == 1

    def test_exit_code_no_deps_returns_one(self):
        data = {"name": "solo", "tasks": [{"task_id": "only"}], "edges": []}
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as fh:
            json.dump(data, fh)
            solo_path = fh.name
        try:
            args = _make_args(file=solo_path, task="only", exit_code=True)
            assert depend_command(args) == 1
        finally:
            os.unlink(solo_path)

    def test_exit_code_with_deps_returns_zero(self):
        args = _make_args(file=self.tmp.name, task="b", exit_code=True)
        assert depend_command(args) == 0
