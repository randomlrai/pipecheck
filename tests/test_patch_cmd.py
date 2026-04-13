"""Tests for pipecheck.commands.patch_cmd."""
import json
import os
import tempfile
import pytest
from argparse import ArgumentParser
from unittest.mock import patch
from pipecheck.commands.patch_cmd import add_patch_subparser, patch_command


def _make_subparsers():
    parser = ArgumentParser(prog="pipecheck")
    sub = parser.add_subparsers(dest="command")
    return parser, sub


def _write_dag(path: str) -> None:
    dag = {
        "name": "test_dag",
        "tasks": [
            {"task_id": "ingest", "description": "Load", "timeout": 30},
            {"task_id": "transform", "description": "Clean", "timeout": 60},
        ],
        "edges": [["ingest", "transform"]],
    }
    with open(path, "w") as fh:
        json.dump(dag, fh)


def _make_args(dag_file: str, spec: str, exit_code: bool = False):
    parser, sub = _make_subparsers()
    add_patch_subparser(sub)
    argv = ["patch", dag_file, "--spec", spec]
    if exit_code:
        argv.append("--exit-code")
    return parser.parse_args(argv)


class TestAddPatchSubparser:
    def test_subparser_registered(self):
        _, sub = _make_subparsers()
        add_patch_subparser(sub)
        parser = ArgumentParser()
        p2 = parser.add_subparsers()
        add_patch_subparser(p2)
        # just ensure no exception

    def test_default_exit_code_false(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            _write_dag(path)
            args = _make_args(path, '{"ingest": {"timeout": 5}}')
            assert args.exit_code is False
        finally:
            os.unlink(path)

    def test_exit_code_flag_sets_true(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            _write_dag(path)
            args = _make_args(path, '{"ingest": {"timeout": 5}}', exit_code=True)
            assert args.exit_code is True
        finally:
            os.unlink(path)


class TestPatchCommand:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self.path = self.tmp.name
        self.tmp.close()
        _write_dag(self.path)

    def teardown_method(self):
        os.unlink(self.path)

    def test_valid_patch_exits_zero(self):
        args = _make_args(self.path, '{"ingest": {"timeout": 99}}')
        assert patch_command(args) == 0

    def test_empty_spec_exits_zero(self):
        args = _make_args(self.path, '{}')
        assert patch_command(args) == 0

    def test_empty_spec_with_exit_code_flag_returns_one(self):
        args = _make_args(self.path, '{}', exit_code=True)
        assert patch_command(args) == 1

    def test_invalid_json_spec_returns_one(self):
        args = _make_args(self.path, 'not-json')
        assert patch_command(args) == 1

    def test_missing_task_returns_one(self):
        args = _make_args(self.path, '{"ghost": {"timeout": 1}}')
        assert patch_command(args) == 1

    def test_missing_field_returns_one(self):
        args = _make_args(self.path, '{"ingest": {"bad_field": 1}}')
        assert patch_command(args) == 1

    def test_bad_dag_file_returns_one(self):
        args = _make_args("/nonexistent/file.json", '{}')
        assert patch_command(args) == 1
