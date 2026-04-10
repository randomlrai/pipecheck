"""Tests for pipecheck.commands.clone_cmd."""
from __future__ import annotations

import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.clone_cmd import add_clone_subparser, clone_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str, name: str = "pipe") -> None:
    payload = {
        "name": name,
        "tasks": [
            {"task_id": "a", "description": "Task A", "timeout": 10},
            {"task_id": "b", "description": "Task B", "timeout": 20, "dependencies": ["a"]},
        ],
    }
    with open(path, "w") as fh:
        json.dump(payload, fh)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "new_name": "cloned",
        "prefix": None,
        "output": None,
        "func": clone_command,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddCloneSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_clone_subparser(subparsers)
        choices = list(subparsers.choices.keys())
        assert "clone" in choices

    def test_default_prefix_is_none(self):
        _, subparsers = _make_subparsers()
        add_clone_subparser(subparsers)
        ns = subparsers.choices["clone"].parse_args(["f.json", "new_dag"])
        assert ns.prefix is None

    def test_prefix_flag(self):
        _, subparsers = _make_subparsers()
        add_clone_subparser(subparsers)
        ns = subparsers.choices["clone"].parse_args(["f.json", "new_dag", "--prefix", "v2_"])
        assert ns.prefix == "v2_"

    def test_output_flag(self):
        _, subparsers = _make_subparsers()
        add_clone_subparser(subparsers)
        ns = subparsers.choices["clone"].parse_args(["f.json", "new_dag", "--output", "out.json"])
        assert ns.output == "out.json"


class TestCloneCommand:
    def test_returns_0_on_success(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as fh:
            _write_dag(fh.name)
            path = fh.name
        try:
            args = _make_args(file=path, new_name="cloned")
            rc = clone_command(args)
            assert rc == 0
        finally:
            os.unlink(path)

    def test_output_contains_new_name(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as fh:
            _write_dag(fh.name)
            path = fh.name
        try:
            args = _make_args(file=path, new_name="my_clone")
            clone_command(args)
            captured = capsys.readouterr().out
            data = json.loads(captured)
            assert data["name"] == "my_clone"
        finally:
            os.unlink(path)

    def test_prefix_applied_in_output(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as fh:
            _write_dag(fh.name)
            path = fh.name
        try:
            args = _make_args(file=path, new_name="cloned", prefix="v2_")
            clone_command(args)
            captured = capsys.readouterr().out
            data = json.loads(captured)
            task_ids = [t["task_id"] for t in data["tasks"]]
            assert all(tid.startswith("v2_") for tid in task_ids)
        finally:
            os.unlink(path)

    def test_missing_file_returns_1(self, capsys):
        args = _make_args(file="nonexistent.json", new_name="cloned")
        rc = clone_command(args)
        assert rc == 1

    def test_writes_to_output_file(self, tmp_path):
        dag_file = tmp_path / "dag.json"
        out_file = tmp_path / "out.json"
        _write_dag(str(dag_file))
        args = _make_args(file=str(dag_file), new_name="cloned", output=str(out_file))
        rc = clone_command(args)
        assert rc == 0
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        assert data["name"] == "cloned"
