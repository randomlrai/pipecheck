"""Tests for pipecheck.commands.match_cmd."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.match_cmd import add_match_subparser, match_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str) -> None:
    dag = {
        "name": "test_dag",
        "tasks": [
            {"task_id": "extract_data"},
            {"task_id": "extract_meta"},
            {"task_id": "transform_data"},
            {"task_id": "load_output"},
        ],
        "edges": [],
    }
    with open(path, "w") as f:
        json.dump(dag, f)


def _make_args(**kwargs):
    defaults = {
        "dag_file": "dag.json",
        "pattern": "*",
        "mode": "glob",
        "ids_only": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddMatchSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_match_subparser(subparsers)
        assert "match" in subparsers.choices

    def test_default_mode_is_glob(self):
        _, subparsers = _make_subparsers()
        add_match_subparser(subparsers)
        ns = subparsers.choices["match"].parse_args(["dag.json", "*"])
        assert ns.mode == "glob"

    def test_mode_flag(self):
        _, subparsers = _make_subparsers()
        add_match_subparser(subparsers)
        ns = subparsers.choices["match"].parse_args(["dag.json", ".*", "--mode", "regex"])
        assert ns.mode == "regex"

    def test_ids_only_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_match_subparser(subparsers)
        ns = subparsers.choices["match"].parse_args(["dag.json", "*"])
        assert ns.ids_only is False

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_match_subparser(subparsers)
        ns = subparsers.choices["match"].parse_args(["dag.json", "*"])
        assert ns.exit_code is False


class TestMatchCommand:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        _write_dag(self.tmp.name)

    def teardown_method(self):
        os.unlink(self.tmp.name)

    def test_glob_match_exits_zero(self):
        args = _make_args(dag_file=self.tmp.name, pattern="extract_*")
        assert match_command(args) == 0

    def test_no_match_exits_zero_without_flag(self):
        args = _make_args(dag_file=self.tmp.name, pattern="missing_*")
        assert match_command(args) == 0

    def test_no_match_exits_one_with_flag(self):
        args = _make_args(dag_file=self.tmp.name, pattern="missing_*", exit_code=True)
        assert match_command(args) == 1

    def test_invalid_dag_file_exits_one(self):
        args = _make_args(dag_file="/nonexistent/dag.json")
        assert match_command(args) == 1

    def test_invalid_regex_exits_one(self):
        args = _make_args(dag_file=self.tmp.name, pattern="[bad", mode="regex")
        assert match_command(args) == 1

    def test_ids_only_prints_ids(self, capsys):
        args = _make_args(dag_file=self.tmp.name, pattern="extract_*", ids_only=True)
        match_command(args)
        captured = capsys.readouterr()
        lines = captured.out.strip().splitlines()
        assert all(l.startswith("extract_") for l in lines)
