"""Tests for pipecheck.commands.segment_cmd."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.segment_cmd import add_segment_subparser, segment_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str, name: str = "pipe") -> None:
    data = {
        "name": name,
        "tasks": [
            {"task_id": "a", "name": "A"},
            {"task_id": "b", "name": "B", "dependencies": ["a"]},
            {"task_id": "c", "name": "C", "dependencies": ["b"]},
        ],
    }
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_args(file: str, size: int = 1, exit_code: bool = False):
    ns = argparse.Namespace()
    ns.file = file
    ns.size = size
    ns.exit_code = exit_code
    return ns


class TestAddSegmentSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_segment_subparser(subs)
        # no exception means success

    def test_default_size_is_one(self):
        parser, subs = _make_subparsers()
        add_segment_subparser(subs)
        args = parser.parse_args(["segment", "dummy.json"])
        assert args.size == 1

    def test_size_flag_parsed(self):
        parser, subs = _make_subparsers()
        add_segment_subparser(subs)
        args = parser.parse_args(["segment", "dummy.json", "--size", "3"])
        assert args.size == 3

    def test_exit_code_default_false(self):
        parser, subs = _make_subparsers()
        add_segment_subparser(subs)
        args = parser.parse_args(["segment", "dummy.json"])
        assert args.exit_code is False

    def test_exit_code_flag_sets_true(self):
        parser, subs = _make_subparsers()
        add_segment_subparser(subs)
        args = parser.parse_args(["segment", "dummy.json", "--exit-code"])
        assert args.exit_code is True


class TestSegmentCommand:
    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self._tmp.close()
        _write_dag(self._tmp.name)

    def teardown_method(self):
        os.unlink(self._tmp.name)

    def test_exits_zero_on_valid_dag(self):
        args = _make_args(self._tmp.name)
        assert segment_command(args) == 0

    def test_exits_one_on_missing_file(self):
        args = _make_args("nonexistent.json")
        assert segment_command(args) == 1

    def test_size_two_returns_two_segments(self, capsys):
        args = _make_args(self._tmp.name, size=2)
        rc = segment_command(args)
        assert rc == 0
        captured = capsys.readouterr()
        assert "seg_" in captured.out

    def test_invalid_size_exits_one(self):
        args = _make_args(self._tmp.name, size=0)
        assert segment_command(args) == 1

    def test_output_contains_dag_name(self, capsys):
        args = _make_args(self._tmp.name)
        segment_command(args)
        captured = capsys.readouterr()
        assert "pipe" in captured.out
