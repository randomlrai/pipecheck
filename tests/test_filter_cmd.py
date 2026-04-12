"""Tests for pipecheck.commands.filter_cmd."""
from __future__ import annotations
import argparse
import json
import os
import tempfile
import pytest
from pipecheck.commands.filter_cmd import add_filter_subparser, filter_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    data = {
        "name": "sample",
        "tasks": [
            {"task_id": "ingest", "timeout": 30,
             "metadata": {"tags": ["io"]}},
            {"task_id": "transform", "timeout": 120,
             "metadata": {"tags": ["compute"]}},
            {"task_id": "notify", "metadata": {"tags": []}},
        ],
        "edges": [["ingest", "transform"]],
    }
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_args(dag_file, **kwargs):
    ns = argparse.Namespace(
        dag_file=dag_file,
        tag=None,
        prefix=None,
        max_timeout=None,
        no_timeout=False,
        quiet=False,
    )
    for k, v in kwargs.items():
        setattr(ns, k, v)
    return ns


class TestAddFilterSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_filter_subparser(subs)
        assert "filter" in subs.choices

    def test_func_set_to_filter_command(self):
        _, subs = _make_subparsers()
        add_filter_subparser(subs)
        parsed = subs.choices["filter"].parse_args(
            ["dag.json", "--tag", "io"]
        )
        assert parsed.func is filter_command

    def test_tag_flag_parsed(self):
        _, subs = _make_subparsers()
        add_filter_subparser(subs)
        parsed = subs.choices["filter"].parse_args(
            ["dag.json", "--tag", "compute"]
        )
        assert parsed.tag == "compute"

    def test_prefix_flag_parsed(self):
        _, subs = _make_subparsers()
        add_filter_subparser(subs)
        parsed = subs.choices["filter"].parse_args(
            ["dag.json", "--prefix", "in"]
        )
        assert parsed.prefix == "in"

    def test_quiet_flag_default_false(self):
        _, subs = _make_subparsers()
        add_filter_subparser(subs)
        parsed = subs.choices["filter"].parse_args(
            ["dag.json", "--no-timeout"]
        )
        assert parsed.quiet is False


class TestFilterCommand:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False, mode="w"
        )
        _write_dag(self.tmp.name)
        self.tmp.close()

    def teardown_method(self):
        os.unlink(self.tmp.name)

    def test_by_tag_exit_zero_when_matches(self):
        args = _make_args(self.tmp.name, tag="io")
        assert filter_command(args) == 0

    def test_by_tag_exit_two_when_no_matches(self):
        args = _make_args(self.tmp.name, tag="missing_tag")
        assert filter_command(args) == 2

    def test_by_prefix_exit_zero(self):
        args = _make_args(self.tmp.name, prefix="in")
        assert filter_command(args) == 0

    def test_by_max_timeout_exit_zero(self):
        args = _make_args(self.tmp.name, max_timeout=30)
        assert filter_command(args) == 0

    def test_by_no_timeout_exit_zero(self):
        args = _make_args(self.tmp.name, no_timeout=True)
        assert filter_command(args) == 0

    def test_bad_file_returns_one(self):
        args = _make_args("/nonexistent/path.json", tag="io")
        assert filter_command(args) == 1
