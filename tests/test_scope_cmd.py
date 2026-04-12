"""Tests for pipecheck.commands.scope_cmd."""
import argparse
import json
import os
import tempfile
import pytest

from pipecheck.commands.scope_cmd import add_scope_subparser, scope_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    dag = {
        "name": "pipe",
        "tasks": [
            {"task_id": "ingest", "metadata": {"scope": "io"}},
            {"task_id": "process", "metadata": {"scope": "core"}},
            {"task_id": "export", "metadata": {"scope": "io"}},
        ],
        "dependencies": [["ingest", "process"], ["process", "export"]],
    }
    with open(path, "w") as f:
        json.dump(dag, f)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "scope_name": "io",
        "list_all": False,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddScopeSubparser:
    def test_subparser_registered(self):
        _, subs = _make_subparsers()
        add_scope_subparser(subs)
        assert "scope" in subs.choices

    def test_default_exit_code_false(self):
        _, subs = _make_subparsers()
        add_scope_subparser(subs)
        ns = subs.choices["scope"].parse_args(["dag.json", "io"])
        assert ns.exit_code is False

    def test_default_list_all_false(self):
        _, subs = _make_subparsers()
        add_scope_subparser(subs)
        ns = subs.choices["scope"].parse_args(["dag.json", "io"])
        assert ns.list_all is False


class TestScopeCommand:
    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self._tmp.close()
        _write_dag(self._tmp.name)

    def teardown_method(self):
        os.unlink(self._tmp.name)

    def test_exits_zero_with_matching_scope(self, capsys):
        args = _make_args(file=self._tmp.name, scope_name="io")
        rc = scope_command(args)
        assert rc == 0

    def test_output_contains_scope_name(self, capsys):
        args = _make_args(file=self._tmp.name, scope_name="io")
        scope_command(args)
        out = capsys.readouterr().out
        assert "io" in out

    def test_exits_zero_for_unknown_scope_without_flag(self, capsys):
        args = _make_args(file=self._tmp.name, scope_name="missing")
        rc = scope_command(args)
        assert rc == 0

    def test_exits_one_for_unknown_scope_with_flag(self, capsys):
        args = _make_args(file=self._tmp.name, scope_name="missing", exit_code=True)
        rc = scope_command(args)
        assert rc == 1

    def test_list_all_exits_zero(self, capsys):
        args = _make_args(file=self._tmp.name, scope_name="", list_all=True)
        rc = scope_command(args)
        assert rc == 0

    def test_list_all_output_contains_labels(self, capsys):
        args = _make_args(file=self._tmp.name, scope_name="", list_all=True)
        scope_command(args)
        out = capsys.readouterr().out
        assert "io" in out
        assert "core" in out
