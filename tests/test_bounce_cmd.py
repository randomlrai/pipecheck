"""Tests for pipecheck.commands.bounce_cmd."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.bounce_cmd import add_bounce_subparser, bounce_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    data = {
        "name": "pipe",
        "tasks": [
            {"task_id": "extract", "metadata": {"tags": ["io"]}},
            {"task_id": "transform", "metadata": {"tags": ["compute"]}},
            {"task_id": "load", "metadata": {"tags": ["io"]}},
        ],
        "edges": [
            {"from": "extract", "to": "transform"},
            {"from": "transform", "to": "load"},
        ],
    }
    with open(path, "w") as f:
        json.dump(data, f)


def _make_args(**kwargs):
    defaults = {
        "dag_file": "dag.json",
        "allow_tags": [],
        "deny_tags": [],
        "allow_prefixes": [],
        "deny_prefixes": [],
        "exit_code": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddBounceSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_bounce_subparser(subs)
        # no exception means success

    def test_default_exit_code_false(self):
        parser, subs = _make_subparsers()
        add_bounce_subparser(subs)
        args = parser.parse_args(["bounce", "dag.json"])
        assert args.exit_code is False

    def test_exit_code_flag(self):
        parser, subs = _make_subparsers()
        add_bounce_subparser(subs)
        args = parser.parse_args(["bounce", "dag.json", "--exit-code"])
        assert args.exit_code is True

    def test_allow_tag_appends(self):
        parser, subs = _make_subparsers()
        add_bounce_subparser(subs)
        args = parser.parse_args(["bounce", "dag.json", "--allow-tag", "io"])
        assert "io" in args.allow_tags

    def test_deny_tag_appends(self):
        parser, subs = _make_subparsers()
        add_bounce_subparser(subs)
        args = parser.parse_args(["bounce", "dag.json", "--deny-tag", "compute"])
        assert "compute" in args.deny_tags


class TestBounceCommand:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self.tmp.close()
        _write_dag(self.tmp.name)

    def teardown_method(self):
        os.unlink(self.tmp.name)

    def test_no_rules_exits_zero(self):
        args = _make_args(dag_file=self.tmp.name)
        assert bounce_command(args) == 0

    def test_deny_tag_exits_zero_without_flag(self):
        args = _make_args(dag_file=self.tmp.name, deny_tags=["io"])
        assert bounce_command(args) == 0

    def test_deny_tag_exits_one_with_flag(self):
        args = _make_args(
            dag_file=self.tmp.name, deny_tags=["io"], exit_code=True
        )
        assert bounce_command(args) == 1

    def test_allow_tag_no_match_exits_one_with_flag(self):
        args = _make_args(
            dag_file=self.tmp.name, allow_tags=["missing_tag"], exit_code=True
        )
        assert bounce_command(args) == 1

    def test_allow_tag_full_match_exits_zero(self):
        args = _make_args(
            dag_file=self.tmp.name,
            allow_tags=["io", "compute"],
            exit_code=True,
        )
        assert bounce_command(args) == 0
