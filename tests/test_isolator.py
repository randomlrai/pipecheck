"""Tests for pipecheck.isolator and the isolate CLI command."""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from unittest.mock import patch

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.isolator import DAGIsolator, IsolateError, IsolateResult
from pipecheck.commands.isolate_cmd import add_isolate_subparser, isolate_command


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    dag.add_task(Task("ingest", "Ingest"))
    dag.add_task(Task("clean", "Clean", dependencies=["ingest"]))
    dag.add_task(Task("transform", "Transform", dependencies=["clean"]))
    dag.add_task(Task("report", "Report", dependencies=["transform"]))
    dag.add_task(Task("notify", "Notify", dependencies=["report"]))
    dag.add_task(Task("archive", "Archive"))  # disconnected root
    return dag


class TestIsolateResult:
    def test_count_property(self):
        dag = DAG(name="sub")
        result = IsolateResult(
            original_name="orig",
            roots=["a"],
            subgraph=dag,
            included_task_ids=["a", "b", "c"],
        )
        assert result.count == 3

    def test_count_empty(self):
        dag = DAG(name="sub")
        result = IsolateResult(original_name="orig", roots=[], subgraph=dag)
        assert result.count == 0


class TestDAGIsolator:
    def setup_method(self):
        self.dag = _build_dag()
        self.isolator = DAGIsolator()

    def test_isolate_single_root_includes_ancestors(self):
        result = self.isolator.isolate(self.dag, ["transform"])
        assert set(result.included_task_ids) == {"ingest", "clean", "transform"}

    def test_isolate_leaf_root_includes_all(self):
        result = self.isolator.isolate(self.dag, ["notify"])
        assert set(result.included_task_ids) == {
            "ingest", "clean", "transform", "report", "notify"
        }

    def test_isolate_root_with_no_deps(self):
        result = self.isolator.isolate(self.dag, ["ingest"])
        assert result.included_task_ids == ["ingest"]

    def test_isolate_disconnected_task(self):
        result = self.isolator.isolate(self.dag, ["archive"])
        assert result.included_task_ids == ["archive"]

    def test_subgraph_preserves_dependencies(self):
        result = self.isolator.isolate(self.dag, ["clean"])
        clean_task = result.subgraph.tasks["clean"]
        assert "ingest" in clean_task.dependencies

    def test_subgraph_drops_out_of_scope_deps(self):
        result = self.isolator.isolate(self.dag, ["transform"])
        # transform depends on clean; clean depends on ingest — all included
        assert "clean" in result.subgraph.tasks["transform"].dependencies

    def test_missing_root_raises(self):
        with pytest.raises(IsolateError, match="not found"):
            self.isolator.isolate(self.dag, ["nonexistent"])

    def test_result_original_name(self):
        result = self.isolator.isolate(self.dag, ["ingest"])
        assert result.original_name == "pipeline"

    def test_subgraph_name_derived(self):
        result = self.isolator.isolate(self.dag, ["ingest"])
        assert result.subgraph.name == "pipeline_isolated"


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str) -> None:
    data = json.dumps({
        "name": "mypipe",
        "tasks": [
            {"task_id": "a", "name": "A"},
            {"task_id": "b", "name": "B", "dependencies": ["a"]},
        ],
    })
    with open(path, "w") as fh:
        fh.write(data)


class TestAddIsolateSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_isolate_subparser(subparsers)
        parser, _ = _make_subparsers()
        add_isolate_subparser(parser.add_subparsers(dest="command"))
        ns = parser.parse_args(["isolate", "dag.json", "a"])
        assert ns.command == "isolate"

    def test_default_output_not_json(self):
        parser = argparse.ArgumentParser()
        sp = parser.add_subparsers(dest="command")
        add_isolate_subparser(sp)
        ns = parser.parse_args(["isolate", "f.json", "root"])
        assert ns.output_json is False

    def test_json_flag(self):
        parser = argparse.ArgumentParser()
        sp = parser.add_subparsers(dest="command")
        add_isolate_subparser(sp)
        ns = parser.parse_args(["isolate", "f.json", "root", "--json"])
        assert ns.output_json is True

    def test_isolate_command_returns_zero(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            f.write(json.dumps({
                "name": "p",
                "tasks": [
                    {"task_id": "x", "name": "X"},
                    {"task_id": "y", "name": "Y", "dependencies": ["x"]},
                ],
            }))
            fname = f.name
        try:
            ns = argparse.Namespace(
                dag_file=fname, roots=["y"], output_json=False
            )
            assert isolate_command(ns) == 0
        finally:
            os.unlink(fname)

    def test_isolate_command_json_output(self, capsys):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            f.write(json.dumps({
                "name": "p",
                "tasks": [
                    {"task_id": "x", "name": "X"},
                    {"task_id": "y", "name": "Y", "dependencies": ["x"]},
                ],
            }))
            fname = f.name
        try:
            ns = argparse.Namespace(
                dag_file=fname, roots=["y"], output_json=True
            )
            rc = isolate_command(ns)
            assert rc == 0
            out = capsys.readouterr().out
            payload = json.loads(out)
            assert "x" in payload["included_tasks"]
            assert "y" in payload["included_tasks"]
        finally:
            os.unlink(fname)

    def test_isolate_command_bad_root_returns_one(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            f.write(json.dumps({
                "name": "p",
                "tasks": [{"task_id": "x", "name": "X"}],
            }))
            fname = f.name
        try:
            ns = argparse.Namespace(
                dag_file=fname, roots=["missing"], output_json=False
            )
            assert isolate_command(ns) == 1
        finally:
            os.unlink(fname)
