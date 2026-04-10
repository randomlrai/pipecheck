"""Tests for pipecheck.commands.rename_cmd."""
import argparse
import json
import os
import tempfile
from unittest.mock import MagicMock, patch
import pytest
from pipecheck.commands.rename_cmd import add_rename_subparser, rename_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    dag = {
        "name": "pipeline",
        "tasks": [
            {"task_id": "extract", "name": "Extract", "depends_on": []},
            {"task_id": "transform", "name": "Transform", "depends_on": ["extract"]},
            {"task_id": "load", "name": "Load", "depends_on": ["transform"]},
        ],
    }
    with open(path, "w") as fh:
        json.dump(dag, fh)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "map": ["extract=ingest"],
        "snapshot": None,
        "quiet": False,
        "func": rename_command,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddRenameSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_rename_subparser(subs)
        assert "rename" in subs.choices

    def test_map_flag_required(self):
        _, subs = _make_subparsers()
        add_rename_subparser(subs)
        parser = subs.choices["rename"]
        with pytest.raises(SystemExit):
            parser.parse_args(["dag.json"])

    def test_quiet_default_false(self):
        _, subs = _make_subparsers()
        add_rename_subparser(subs)
        parser = subs.choices["rename"]
        ns = parser.parse_args(["dag.json", "--map", "a=b"])
        assert ns.quiet is False


class TestRenameCommand:
    def test_successful_rename_returns_zero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag_file = os.path.join(tmpdir, "dag.json")
            _write_dag(dag_file)
            args = _make_args(file=dag_file, map=["extract=ingest"])
            assert rename_command(args) == 0

    def test_missing_file_returns_one(self):
        args = _make_args(file="/nonexistent/dag.json")
        assert rename_command(args) == 1

    def test_invalid_map_entry_returns_one(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag_file = os.path.join(tmpdir, "dag.json")
            _write_dag(dag_file)
            args = _make_args(file=dag_file, map=["badentry"])
            assert rename_command(args) == 1

    def test_unknown_task_returns_one(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag_file = os.path.join(tmpdir, "dag.json")
            _write_dag(dag_file)
            args = _make_args(file=dag_file, map=["ghost=new_id"])
            assert rename_command(args) == 1

    def test_snapshot_saved_when_path_given(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag_file = os.path.join(tmpdir, "dag.json")
            snap_file = os.path.join(tmpdir, "snap.json")
            _write_dag(dag_file)
            args = _make_args(file=dag_file, map=["extract=ingest"], snapshot=snap_file)
            rename_command(args)
            assert os.path.exists(snap_file)

    def test_quiet_suppresses_output(self, capsys):
        with tempfile.TemporaryDirectory() as tmpdir:
            dag_file = os.path.join(tmpdir, "dag.json")
            _write_dag(dag_file)
            args = _make_args(file=dag_file, map=["extract=ingest"], quiet=True)
            rename_command(args)
            captured = capsys.readouterr()
            assert captured.out == ""
