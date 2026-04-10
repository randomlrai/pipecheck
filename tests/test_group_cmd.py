"""Tests for pipecheck.commands.group_cmd."""
import argparse
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from pipecheck.commands.group_cmd import add_group_subparser, group_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(path: str) -> None:
    data = {
        "name": "sample",
        "tasks": [
            {"task_id": "ingest_raw", "metadata": {"tags": ["ingest"]}},
            {"task_id": "transform_clean", "metadata": {"tags": ["transform"]}},
        ],
        "edges": [],
    }
    with open(path, "w") as f:
        json.dump(data, f)


def _make_args(**kwargs):
    defaults = {"dag_file": "dag.json", "by": "tag", "separator": "_", "func": group_command}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddGroupSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_group_subparser(subparsers)
        assert "group" in subparsers.choices

    def test_default_by_is_tag(self):
        _, subparsers = _make_subparsers()
        add_group_subparser(subparsers)
        ns = subparsers.choices["group"].parse_args(["dag.json"])
        assert ns.by == "tag"

    def test_by_prefix_flag(self):
        _, subparsers = _make_subparsers()
        add_group_subparser(subparsers)
        ns = subparsers.choices["group"].parse_args(["dag.json", "--by", "prefix"])
        assert ns.by == "prefix"

    def test_separator_flag(self):
        _, subparsers = _make_subparsers()
        add_group_subparser(subparsers)
        ns = subparsers.choices["group"].parse_args(["dag.json", "--separator", "-"])
        assert ns.separator == "-"


class TestGroupCommand:
    def test_returns_zero_on_success_by_tag(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            _write_dag(f.name)
            name = f.name
        try:
            args = _make_args(dag_file=name, by="tag")
            rc = group_command(args)
            assert rc == 0
        finally:
            os.unlink(name)

    def test_returns_zero_on_success_by_prefix(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            _write_dag(f.name)
            name = f.name
        try:
            args = _make_args(dag_file=name, by="prefix", separator="_")
            rc = group_command(args)
            assert rc == 0
        finally:
            os.unlink(name)

    def test_group_error_returns_one(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            _write_dag(f.name)
            name = f.name
        try:
            args = _make_args(dag_file=name, by="prefix", separator="")
            rc = group_command(args)
            assert rc == 1
        finally:
            os.unlink(name)

    def test_output_contains_group_info(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            _write_dag(f.name)
            name = f.name
        try:
            args = _make_args(dag_file=name, by="tag")
            group_command(args)
            captured = capsys.readouterr()
            assert "ingest" in captured.out or "GroupResult" in captured.out
        finally:
            os.unlink(name)
