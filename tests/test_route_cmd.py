"""Tests for pipecheck.commands.route_cmd."""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from pipecheck.commands.route_cmd import add_route_subparser, route_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    dag_data = {
        "name": "test_dag",
        "tasks": [
            {"task_id": "a"},
            {"task_id": "b", "dependencies": ["a"]},
            {"task_id": "c", "dependencies": ["a"]},
            {"task_id": "d", "dependencies": ["b", "c"]},
        ],
    }
    with open(path, "w") as f:
        json.dump(dag_data, f)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "source": "a",
        "target": "d",
        "shortest": False,
        "longest": False,
        "func": route_command,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddRouteSubparser:
    def test_subparser_registered(self):
        sp = _make_subparsers()
        add_route_subparser(sp)
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_route_subparser(subs)
        ns = parser.parse_args(["route", "dag.json", "a", "d"])
        assert ns.command == "route"

    def test_shortest_flag_default_false(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_route_subparser(subs)
        ns = parser.parse_args(["route", "dag.json", "a", "d"])
        assert ns.shortest is False

    def test_longest_flag_can_be_set(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers(dest="command")
        add_route_subparser(subs)
        ns = parser.parse_args(["route", "dag.json", "a", "d", "--longest"])
        assert ns.longest is True


class TestRouteCommand:
    def test_returns_zero_on_success(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            fname = f.name
        try:
            _write_dag(fname)
            args = _make_args(file=fname)
            rc = route_command(args)
            assert rc == 0
        finally:
            os.unlink(fname)

    def test_returns_one_on_bad_file(self):
        args = _make_args(file="/nonexistent/dag.json")
        rc = route_command(args)
        assert rc == 1

    def test_returns_one_on_unknown_source(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            fname = f.name
        try:
            _write_dag(fname)
            args = _make_args(file=fname, source="zzz")
            rc = route_command(args)
            assert rc == 1
        finally:
            os.unlink(fname)

    def test_shortest_flag_prints_shortest(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            fname = f.name
        try:
            _write_dag(fname)
            args = _make_args(file=fname, shortest=True)
            rc = route_command(args)
            assert rc == 0
            captured = capsys.readouterr()
            assert "Shortest path" in captured.out
        finally:
            os.unlink(fname)

    def test_no_route_exits_zero(self, capsys):
        dag_data = {
            "name": "disconnected",
            "tasks": [{"task_id": "a"}, {"task_id": "b"}],
        }
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            json.dump(dag_data, f)
            fname = f.name
        try:
            args = _make_args(file=fname, source="a", target="b")
            rc = route_command(args)
            assert rc == 0
            captured = capsys.readouterr()
            assert "No path found" in captured.out
        finally:
            os.unlink(fname)
