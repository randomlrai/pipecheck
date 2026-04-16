"""Tests for pipecheck.commands.feed_cmd."""
import json
import os
import tempfile
import pytest
from argparse import ArgumentParser
from pipecheck.commands.feed_cmd import add_feed_subparser, feed_command


def _make_subparsers():
    parser = ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str) -> None:
    dag = {
        "name": "pipe",
        "tasks": [
            {"task_id": "ingest", "command": "run_ingest"},
            {"task_id": "load", "command": "run_load"},
        ],
        "edges": [],
    }
    with open(path, "w") as f:
        json.dump(dag, f)


def _write_feed(path: str, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f)


def _make_args(dag_file, feed_file, source="ext", exit_code=False):
    class A:
        pass
    a = A()
    a.dag_file = dag_file
    a.feed_file = feed_file
    a.source = source
    a.exit_code = exit_code
    return a


class TestAddFeedSubparser:
    def test_subparser_registered(self):
        _, sub = _make_subparsers()
        add_feed_subparser(sub)

    def test_default_source(self):
        parser, sub = _make_subparsers()
        add_feed_subparser(sub)
        args = parser.parse_args(["feed", "dag.json", "feed.json"])
        assert args.source == "external"

    def test_exit_code_flag_default_false(self):
        parser, sub = _make_subparsers()
        add_feed_subparser(sub)
        args = parser.parse_args(["feed", "dag.json", "feed.json"])
        assert args.exit_code is False


class TestFeedCommand:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.dag_file = os.path.join(self.tmp, "dag.json")
        self.feed_file = os.path.join(self.tmp, "feed.json")
        _write_dag(self.dag_file)

    def test_valid_feed_exits_zero(self):
        _write_feed(self.feed_file, {"ingest": {"owner": "alice"}})
        args = _make_args(self.dag_file, self.feed_file)
        assert feed_command(args) == 0

    def test_empty_feed_exits_zero_without_flag(self):
        _write_feed(self.feed_file, {})
        args = _make_args(self.dag_file, self.feed_file)
        assert feed_command(args) == 0

    def test_empty_feed_exits_one_with_flag(self):
        _write_feed(self.feed_file, {})
        args = _make_args(self.dag_file, self.feed_file, exit_code=True)
        assert feed_command(args) == 1

    def test_unknown_task_exits_one(self):
        _write_feed(self.feed_file, {"ghost": {"x": "y"}})
        args = _make_args(self.dag_file, self.feed_file)
        assert feed_command(args) == 1

    def test_bad_dag_file_exits_one(self):
        args = _make_args("/nonexistent/dag.json", self.feed_file)
        assert feed_command(args) == 1

    def test_bad_feed_file_exits_one(self):
        args = _make_args(self.dag_file, "/nonexistent/feed.json")
        assert feed_command(args) == 1
