"""Tests for pipecheck.commands.alias_cmd."""

import argparse
import json
import os
import tempfile

import pytest

from pipecheck.commands.alias_cmd import add_alias_subparser, alias_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(tmp_path: str) -> str:
    dag_data = {
        "name": "test_pipeline",
        "tasks": [
            {"task_id": "ingest", "command": "python ingest.py"},
            {"task_id": "transform", "command": "python transform.py"},
        ],
        "edges": [["ingest", "transform"]],
    }
    path = os.path.join(tmp_path, "dag.json")
    with open(path, "w") as fh:
        json.dump(dag_data, fh)
    return path


def _make_args(dag_file, map_pairs, strict=False):
    ns = argparse.Namespace(
        dag_file=dag_file,
        map=map_pairs,
        strict=strict,
    )
    return ns


class TestAddAliasSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_alias_subparser(subparsers)
        assert "alias" in subparsers.choices

    def test_default_strict_false(self):
        _, subparsers = _make_subparsers()
        add_alias_subparser(subparsers)
        parser = subparsers.choices["alias"]
        args = parser.parse_args(["dag.json", "--map", "ingest=Ingestion"])
        assert args.strict is False

    def test_strict_flag_sets_true(self):
        _, subparsers = _make_subparsers()
        add_alias_subparser(subparsers)
        parser = subparsers.choices["alias"]
        args = parser.parse_args(["dag.json", "--map", "ingest=Ingestion", "--strict"])
        assert args.strict is True

    def test_func_defaults_to_alias_command(self):
        _, subparsers = _make_subparsers()
        add_alias_subparser(subparsers)
        parser = subparsers.choices["alias"]
        args = parser.parse_args(["dag.json", "--map", "a=b"])
        assert args.func is alias_command


class TestAliasCommand:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.dag_file = _write_dag(self.tmp)

    def test_returns_zero_on_success(self):
        args = _make_args(self.dag_file, ["ingest=Ingestion Step"])
        assert alias_command(args) == 0

    def test_returns_zero_with_unresolved_non_strict(self):
        args = _make_args(self.dag_file, ["ghost=Ghost Task"], strict=False)
        assert alias_command(args) == 0

    def test_returns_one_with_unresolved_strict(self):
        args = _make_args(self.dag_file, ["ghost=Ghost Task"], strict=True)
        assert alias_command(args) == 1

    def test_invalid_mapping_format_returns_one(self):
        args = _make_args(self.dag_file, ["bad_format_no_equals"])
        assert alias_command(args) == 1

    def test_missing_dag_file_returns_one(self):
        args = _make_args("/nonexistent/dag.json", ["ingest=Ingestion"])
        assert alias_command(args) == 1

    def test_multiple_mappings_resolved(self):
        args = _make_args(
            self.dag_file,
            ["ingest=Ingestion Step", "transform=Transform Step"],
        )
        assert alias_command(args) == 0
