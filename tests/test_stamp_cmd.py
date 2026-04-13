"""Tests for pipecheck.commands.stamp_cmd."""
import json
import tempfile
import os
from argparse import ArgumentParser
from unittest.mock import patch

import pytest

from pipecheck.commands.stamp_cmd import add_stamp_subparser, stamp_command


def _make_subparsers():
    parser = ArgumentParser(prog="pipecheck")
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    payload = {
        "name": "test_pipe",
        "tasks": [
            {"task_id": "extract"},
            {"task_id": "transform"},
            {"task_id": "load"},
        ],
        "edges": [
            {"from": "extract", "to": "transform"},
            {"from": "transform", "to": "load"},
        ],
    }
    import json as _json
    with open(path, "w") as f:
        _json.dump(payload, f)


def _make_args(dag_file, tasks=None, author=None, output_json=False, exit_code=False):
    from argparse import Namespace
    return Namespace(
        dag_file=dag_file,
        tasks=tasks or [],
        author=author,
        output_json=output_json,
        exit_code=exit_code,
    )


class TestAddStampSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_stamp_subparser(subs)
        parser = ArgumentParser(prog="pipecheck")
        subs2 = parser.add_subparsers(dest="command")
        add_stamp_subparser(subs2)
        args = parser.parse_args(["stamp", "dag.json", "--task", "extract=1.0"])
        assert args.command == "stamp"

    def test_default_author_is_none(self):
        parser = ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_stamp_subparser(subs)
        args = parser.parse_args(["stamp", "f.json"])
        assert args.author is None

    def test_output_json_flag(self):
        parser = ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_stamp_subparser(subs)
        args = parser.parse_args(["stamp", "f.json", "--json"])
        assert args.output_json is True

    def test_exit_code_flag(self):
        parser = ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_stamp_subparser(subs)
        args = parser.parse_args(["stamp", "f.json", "--exit-code"])
        assert args.exit_code is True


class TestStampCommand:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self.tmp.close()
        _write_dag(self.tmp.name)

    def teardown_method(self):
        os.unlink(self.tmp.name)

    def test_stamp_single_task_exits_zero(self, capsys):
        args = _make_args(self.tmp.name, tasks=["extract=1.0.0"])
        code = stamp_command(args)
        assert code == 0

    def test_stamp_output_contains_task(self, capsys):
        args = _make_args(self.tmp.name, tasks=["extract=1.0.0"])
        stamp_command(args)
        out = capsys.readouterr().out
        assert "extract" in out

    def test_stamp_json_output(self, capsys):
        args = _make_args(self.tmp.name, tasks=["load=2.0"], output_json=True)
        stamp_command(args)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data[0]["task_id"] == "load"
        assert data[0]["version"] == "2.0"

    def test_stamp_unknown_task_returns_one(self, capsys):
        args = _make_args(self.tmp.name, tasks=["ghost=1.0"])
        code = stamp_command(args)
        assert code == 1

    def test_stamp_bad_format_returns_one(self, capsys):
        args = _make_args(self.tmp.name, tasks=["badformat"])
        code = stamp_command(args)
        assert code == 1

    def test_stamp_no_tasks_returns_one(self, capsys):
        args = _make_args(self.tmp.name, tasks=[])
        code = stamp_command(args)
        assert code == 1

    def test_stamp_with_author_in_output(self, capsys):
        args = _make_args(self.tmp.name, tasks=["transform=3.0"], author="alice")
        stamp_command(args)
        out = capsys.readouterr().out
        assert "alice" in out
