"""Tests for pipecheck.commands.cut_cmd."""
import argparse
import json
import os
import tempfile
import types

import pytest

from pipecheck.commands.cut_cmd import add_cut_subparser, cut_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser.add_subparsers()


def _write_dag(path: str, name: str = "pipeline") -> None:
    data = {
        "name": name,
        "tasks": [
            {"task_id": "extract"},
            {"task_id": "transform"},
            {"task_id": "load"},
        ],
        "dependencies": {
            "transform": ["extract"],
            "load": ["transform"],
        },
    }
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = {"file": "dag.json", "task": "transform", "exit_code": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddCutSubparser:
    def test_subparser_registered(self):
        subs = _make_subparsers()
        add_cut_subparser(subs)
        assert "cut" in subs.choices

    def test_exit_code_flag_default_false(self):
        subs = _make_subparsers()
        add_cut_subparser(subs)
        ns = subs.choices["cut"].parse_args(["dag.json", "extract"])
        assert ns.exit_code is False

    def test_exit_code_flag_can_be_set(self):
        subs = _make_subparsers()
        add_cut_subparser(subs)
        ns = subs.choices["cut"].parse_args(["dag.json", "extract", "--exit-code"])
        assert ns.exit_code is True

    def test_func_set_to_cut_command(self):
        subs = _make_subparsers()
        add_cut_subparser(subs)
        ns = subs.choices["cut"].parse_args(["dag.json", "t"])
        assert ns.func is cut_command


class TestCutCommand:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self.tmp.close()
        _write_dag(self.tmp.name)

    def teardown_method(self):
        os.unlink(self.tmp.name)

    def test_returns_zero_for_middle_task(self):
        args = _make_args(file=self.tmp.name, task="transform")
        assert cut_command(args) == 0

    def test_returns_zero_for_root_without_exit_code_flag(self):
        args = _make_args(file=self.tmp.name, task="extract", exit_code=False)
        assert cut_command(args) == 0

    def test_returns_one_for_root_with_exit_code_flag(self):
        args = _make_args(file=self.tmp.name, task="extract", exit_code=True)
        assert cut_command(args) == 1

    def test_returns_one_for_leaf_with_exit_code_flag(self):
        args = _make_args(file=self.tmp.name, task="load", exit_code=True)
        assert cut_command(args) == 1

    def test_returns_one_for_missing_file(self):
        args = _make_args(file="/no/such/file.json", task="extract")
        assert cut_command(args) == 1

    def test_returns_one_for_unknown_task(self):
        args = _make_args(file=self.tmp.name, task="nonexistent")
        assert cut_command(args) == 1
