"""Tests for pipecheck.commands.graph_cmd."""
import argparse
import json
import os
import tempfile
import pytest
from pipecheck.commands.graph_cmd import add_graph_subparser, graph_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str, name: str = "test_dag") -> None:
    data = {
        "name": name,
        "tasks": [
            {"task_id": "ingest", "command": "run_ingest"},
            {"task_id": "transform", "command": "run_transform", "dependencies": ["ingest"]},
            {"task_id": "load", "command": "run_load", "dependencies": ["transform"]},
        ],
    }
    with open(path, "w") as f:
        json.dump(data, f)


def _make_args(**kwargs):
    defaults = {"dag_file": "dag.json", "reachable": None, "exit_code": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddGraphSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_graph_subparser(subparsers)
        assert "graph" in subparsers.choices

    def test_default_reachable_is_none(self):
        _, subparsers = _make_subparsers()
        add_graph_subparser(subparsers)
        ns = subparsers.choices["graph"].parse_args(["some.json"])
        assert ns.reachable is None

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_graph_subparser(subparsers)
        ns = subparsers.choices["graph"].parse_args(["some.json"])
        assert ns.exit_code is False

    def test_reachable_flag_parsed(self):
        _, subparsers = _make_subparsers()
        add_graph_subparser(subparsers)
        ns = subparsers.choices["graph"].parse_args(["dag.json", "--reachable", "ingest"])
        assert ns.reachable == "ingest"


class TestGraphCommand:
    def test_returns_zero_on_valid_dag(self, capsys):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "dag.json")
            _write_dag(path)
            args = _make_args(dag_file=path)
            code = graph_command(args)
        assert code == 0

    def test_output_contains_dag_name(self, capsys):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "dag.json")
            _write_dag(path, name="my_pipeline")
            args = _make_args(dag_file=path)
            graph_command(args)
        out = capsys.readouterr().out
        assert "my_pipeline" in out

    def test_returns_one_on_missing_file(self, capsys):
        args = _make_args(dag_file="/nonexistent/path.json")
        code = graph_command(args)
        assert code == 1

    def test_reachable_output_for_valid_task(self, capsys):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "dag.json")
            _write_dag(path)
            args = _make_args(dag_file=path, reachable="ingest")
            code = graph_command(args)
        assert code == 0
        out = capsys.readouterr().out
        assert "transform" in out

    def test_reachable_unknown_task_returns_one(self, capsys):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "dag.json")
            _write_dag(path)
            args = _make_args(dag_file=path, reachable="ghost_task")
            code = graph_command(args)
        assert code == 1
