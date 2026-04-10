"""Tests for pipecheck.commands.link_cmd."""

from __future__ import annotations

import argparse
import json
import textwrap
from pathlib import Path

import pytest

from pipecheck.commands.link_cmd import add_link_subparser, link_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers()


def _write_dag(tmp_path: Path, name: str, task_ids: list[str]) -> Path:
    data = {
        "name": name,
        "tasks": [{"task_id": tid, "name": tid} for tid in task_ids],
        "edges": [],
    }
    p = tmp_path / f"{name}.json"
    p.write_text(json.dumps(data))
    return p


def _write_spec(tmp_path: Path, links: list[dict]) -> Path:
    p = tmp_path / "links.json"
    p.write_text(json.dumps(links))
    return p


def _make_args(**kwargs):
    defaults = {"exit_code": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestAddLinkSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_link_subparser(subparsers)
        assert "link" in subparsers.choices

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_link_subparser(subparsers)
        ns = subparsers.choices["link"].parse_args(["spec.json", "dag.json"])
        assert ns.exit_code is False

    def test_exit_code_flag_true_when_set(self):
        _, subparsers = _make_subparsers()
        add_link_subparser(subparsers)
        ns = subparsers.choices["link"].parse_args(
            ["spec.json", "dag.json", "--exit-code"]
        )
        assert ns.exit_code is True

    def test_func_set_to_link_command(self):
        _, subparsers = _make_subparsers()
        add_link_subparser(subparsers)
        ns = subparsers.choices["link"].parse_args(["spec.json", "dag.json"])
        assert ns.func is link_command


class TestLinkCommand:
    def test_all_resolved_no_exit(self, tmp_path, capsys):
        dag_path = _write_dag(tmp_path, "dag_a", ["task1", "task2"])
        spec_path = _write_spec(
            tmp_path,
            [{"source_dag": "x", "source_task": "y",
              "target_dag": "dag_a", "target_task": "task1"}],
        )
        args = _make_args(spec=str(spec_path), dags=[str(dag_path)])
        link_command(args)  # should not raise
        out = capsys.readouterr().out
        assert "Resolved: 1" in out

    def test_unresolved_triggers_sys_exit(self, tmp_path):
        dag_path = _write_dag(tmp_path, "dag_a", ["task1"])
        spec_path = _write_spec(
            tmp_path,
            [{"source_dag": "x", "source_task": "y",
              "target_dag": "dag_a", "target_task": "ghost"}],
        )
        args = _make_args(
            spec=str(spec_path), dags=[str(dag_path)], exit_code=True
        )
        with pytest.raises(SystemExit) as exc_info:
            link_command(args)
        assert exc_info.value.code == 1

    def test_unresolved_no_exit_code_flag(self, tmp_path):
        dag_path = _write_dag(tmp_path, "dag_a", ["task1"])
        spec_path = _write_spec(
            tmp_path,
            [{"source_dag": "x", "source_task": "y",
              "target_dag": "dag_a", "target_task": "ghost"}],
        )
        args = _make_args(
            spec=str(spec_path), dags=[str(dag_path)], exit_code=False
        )
        link_command(args)  # should NOT raise even with unresolved links
