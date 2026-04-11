"""Tests for pipecheck.commands.partition_cmd."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.partition_cmd import add_partition_subparser, partition_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    dag = {
        "name": "sample",
        "tasks": [
            {"task_id": "load", "metadata": {"tags": ["etl"], "team": "eng"}},
            {"task_id": "clean", "metadata": {"tags": ["etl"], "team": "eng"}},
            {"task_id": "predict", "metadata": {"tags": ["ml"], "team": "ds"}},
            {"task_id": "report", "metadata": {"team": "bi"}},
        ],
        "edges": [["load", "clean"], ["clean", "predict"]],
    }
    with open(path, "w") as fh:
        json.dump(dag, fh)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "by_tag": True,
        "by_key": None,
        "show_unassigned": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddPartitionSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_partition_subparser(subparsers)
        assert "partition" in subparsers.choices

    def test_default_by_tag(self):
        _, subparsers = _make_subparsers()
        add_partition_subparser(subparsers)
        ns = subparsers.choices["partition"].parse_args(["dag.json"])
        assert ns.by_tag is True
        assert ns.by_key is None

    def test_by_key_flag(self):
        _, subparsers = _make_subparsers()
        add_partition_subparser(subparsers)
        ns = subparsers.choices["partition"].parse_args(["dag.json", "--by-key", "team"])
        assert ns.by_key == "team"

    def test_show_unassigned_flag(self):
        _, subparsers = _make_subparsers()
        add_partition_subparser(subparsers)
        ns = subparsers.choices["partition"].parse_args(["dag.json", "--show-unassigned"])
        assert ns.show_unassigned is True


class TestPartitionCommand:
    def test_returns_zero_on_valid_dag(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            _write_dag(f.name)
            path = f.name
        try:
            args = _make_args(file=path)
            assert partition_command(args) == 0
        finally:
            os.unlink(path)

    def test_returns_one_on_bad_file(self):
        args = _make_args(file="/nonexistent/path.json")
        assert partition_command(args) == 1

    def test_by_key_team_exits_zero(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            _write_dag(f.name)
            path = f.name
        try:
            args = _make_args(file=path, by_tag=False, by_key="team")
            assert partition_command(args) == 0
        finally:
            os.unlink(path)

    def test_output_contains_partition_name(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            _write_dag(f.name)
            path = f.name
        try:
            args = _make_args(file=path)
            partition_command(args)
            captured = capsys.readouterr()
            assert "etl" in captured.out
        finally:
            os.unlink(path)

    def test_show_unassigned_prints_unassigned(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            _write_dag(f.name)
            path = f.name
        try:
            args = _make_args(file=path, show_unassigned=True)
            partition_command(args)
            captured = capsys.readouterr()
            # 'report' task has no tags so it should appear as unassigned
            assert "report" in captured.out
        finally:
            os.unlink(path)
