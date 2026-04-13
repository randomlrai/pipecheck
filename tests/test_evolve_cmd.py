"""Tests for pipecheck.commands.evolve_cmd."""
import argparse
import json
import os
import tempfile
import pytest
from pipecheck.commands.evolve_cmd import add_evolve_subparser, evolve_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str, name: str, tasks, edges=None) -> None:
    data = {
        "name": name,
        "tasks": [{"task_id": t} for t in tasks],
        "edges": [list(e) for e in (edges or [])],
    }
    with open(path, "w") as f:
        json.dump(data, f)


def _make_args(baseline, current, exit_code=False):
    ns = argparse.Namespace()
    ns.baseline = baseline
    ns.current = current
    ns.exit_code = exit_code
    return ns


class TestAddEvolveSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_evolve_subparser(subparsers)
        parser = subparsers.choices.get("evolve")
        assert parser is not None

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_evolve_subparser(subparsers)
        parsed = subparsers.choices["evolve"].parse_args(["a.json", "b.json"])
        assert parsed.exit_code is False

    def test_exit_code_flag_can_be_set(self):
        _, subparsers = _make_subparsers()
        add_evolve_subparser(subparsers)
        parsed = subparsers.choices["evolve"].parse_args(["a.json", "b.json", "--exit-code"])
        assert parsed.exit_code is True

    def test_func_set_to_evolve_command(self):
        _, subparsers = _make_subparsers()
        add_evolve_subparser(subparsers)
        parsed = subparsers.choices["evolve"].parse_args(["a.json", "b.json"])
        assert parsed.func is evolve_command


class TestEvolveCommand:
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.baseline = os.path.join(self.tmpdir, "baseline.json")
        self.current = os.path.join(self.tmpdir, "current.json")

    def test_identical_dags_exits_zero(self, capsys):
        _write_dag(self.baseline, "pipe", ["a", "b"], [("a", "b")])
        _write_dag(self.current, "pipe", ["a", "b"], [("a", "b")])
        args = _make_args(self.baseline, self.current)
        code = evolve_command(args)
        assert code == 0

    def test_changed_dag_exits_zero_without_flag(self, capsys):
        _write_dag(self.baseline, "pipe", ["a"])
        _write_dag(self.current, "pipe", ["a", "b"])
        args = _make_args(self.baseline, self.current, exit_code=False)
        code = evolve_command(args)
        assert code == 0

    def test_changed_dag_exits_one_with_flag(self, capsys):
        _write_dag(self.baseline, "pipe", ["a"])
        _write_dag(self.current, "pipe", ["a", "b"])
        args = _make_args(self.baseline, self.current, exit_code=True)
        code = evolve_command(args)
        assert code == 1

    def test_output_contains_change_info(self, capsys):
        _write_dag(self.baseline, "pipe", ["a"])
        _write_dag(self.current, "pipe", ["a", "b"])
        args = _make_args(self.baseline, self.current)
        evolve_command(args)
        captured = capsys.readouterr()
        assert "added_task" in captured.out or "b" in captured.out

    def test_no_changes_output_mentions_no_changes(self, capsys):
        _write_dag(self.baseline, "pipe", ["a"])
        _write_dag(self.current, "pipe", ["a"])
        args = _make_args(self.baseline, self.current)
        evolve_command(args)
        captured = capsys.readouterr()
        assert "No changes" in captured.out
