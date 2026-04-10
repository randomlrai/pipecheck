"""Tests for pipecheck.commands.flatten_cmd."""
from __future__ import annotations

import argparse
import json
import tempfile
import os
from unittest.mock import patch, MagicMock

import pytest

from pipecheck.commands.flatten_cmd import add_flatten_subparser, flatten_command
from pipecheck.dag import DAG, Task
from pipecheck.flattener import FlattenResult, FlatTask


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str) -> None:
    data = {
        "name": "cmd_dag",
        "tasks": [
            {"task_id": "a", "command": "echo a"},
            {"task_id": "b", "command": "echo b", "dependencies": ["a"]},
        ],
    }
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = {"dag_file": "dag.json", "show_metadata": False, "func": flatten_command}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddFlattenSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_flatten_subparser(subs)
        # no exception means it registered fine

    def test_default_show_metadata_false(self):
        parser, subs = _make_subparsers()
        add_flatten_subparser(subs)
        ns = parser.parse_args(["flatten", "some.json"])
        assert ns.show_metadata is False

    def test_show_metadata_flag(self):
        parser, subs = _make_subparsers()
        add_flatten_subparser(subs)
        ns = parser.parse_args(["flatten", "some.json", "--show-metadata"])
        assert ns.show_metadata is True


class TestFlattenCommand:
    def test_returns_zero_on_success(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as fh:
            json.dump(
                {"name": "t", "tasks": [{"task_id": "x", "command": "run"}]}, fh
            )
            tmp =:
            args = _make_args(dag_file=tmp)
            code = flatten_command(args)
            assert code == 0
        finally:
            os.unlink(tmp)

    def test_output_contains_task_id(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as fh:
            json.dump(
                {"name": "t", "tasks": [{"task_id": "my_task", "command": "run"}]}, fh
            )
            tmp = fh.name
        try:
            args = _make_args(dag_file=tmp)
            flatten_command(args)
            captured = capsys.readouterr()
            assert "my_task" in captured.out
        finally:
            os.unlink(tmp)

    def test_missing_file_returns_one(self, capsys):
        args = _make_args(dag_file="/nonexistent/path/dag.json")
        code = flatten_command(args)
        assert code == 1

    def test_show_metadata_flag_prints_meta(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as fh:
            json.dump(
                {
                    "name": "t",
                    "tasks": [
                        {"task_id": "x", "command": "run", "metadata": {"owner": "bob"}}
                    ],
                },
                fh,
            )
            tmp = fh.name
        try:
            args = _make_args(dag_file=tmp, show_metadata=True)
            flatten_command(args)
            captured = capsys.readouterr()
            assert "bob" in captured.out
        finally:
            os.unlink(tmp)
