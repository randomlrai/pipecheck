from __future__ import annotations

import argparse
import json
import os
import tempfile
from unittest.mock import patch

import pytest

from pipecheck.tracer_cmd import add_trace_subparser, trace_command


def _make_subparsers():
    parser = argparse.ArgumentParser()
    return parser, parser.add_subparsers(dest="command")


def _write_dag(path: str, data: dict) -> None:
    with open(path, "w") as fh:
        json.dump(data, fh)


def _make_args(**kwargs):
    defaults = {
        "file": "dag.json",
        "start": "task_a",
        "max_depth": None,
        "exit_code": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


_SIMPLE_DAG = {
    "name": "trace_dag",
    "tasks": [
        {"id": "task_a", "name": "Task A"},
        {"id": "task_b", "name": "Task B", "depends_on": ["task_a"]},
        {"id": "task_c", "name": "Task C", "depends_on": ["task_b"]},
    ],
}


class TestAddTraceSubparser:
    def test_subparser_registered(self):
        _, subparsers = _make_subparsers()
        add_trace_subparser(subparsers)
        assert "trace" in subparsers.choices

    def test_default_max_depth_is_none(self):
        _, subparsers = _make_subparsers()
        add_trace_subparser(subparsers)
        parser = subparsers.choices["trace"]
        args = parser.parse_args(["dag.json", "task_a"])
        assert args.max_depth is None

    def test_exit_code_flag_default_false(self):
        _, subparsers = _make_subparsers()
        add_trace_subparser(subparsers)
        parser = subparsers.choices["trace"]
        args = parser.parse_args(["dag.json", "task_a"])
        assert args.exit_code is False

    def test_max_depth_flag_parsed(self):
        _, subparsers = _make_subparsers()
        add_trace_subparser(subparsers)
        parser = subparsers.choices["trace"]
        args = parser.parse_args(["dag.json", "task_a", "--max-depth", "3"])
        assert args.max_depth == 3


class TestTraceCommand:
    def test_returns_zero_on_valid_dag(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as fh:
            json.dump(_SIMPLE_DAG, fh)
            path = fh.name
        try:
            args = _make_args(file=path, start="task_a")
            assert trace_command(args) == 0
        finally:
            os.unlink(path)

    def test_returns_one_on_missing_file(self):
        args = _make_args(file="/nonexistent/dag.json")
        assert trace_command(args) == 1

    def test_exit_code_flag_nonzero_when_no_steps(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as fh:
            json.dump(_SIMPLE_DAG, fh)
            path = fh.name
        try:
            from pipecheck.tracer import ExecutionTrace
            empty_trace = ExecutionTrace(dag_name="trace_dag", steps=[])
            with patch(
                "pipecheck.tracer_cmd.DAGTracer.trace",
                return_value=empty_trace,
            ):
                args = _make_args(file=path, start="task_a", exit_code=True)
                assert trace_command(args) == 1
        finally:
            os.unlink(path)

    def test_exit_code_flag_zero_when_steps_exist(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as fh:
            json.dump(_SIMPLE_DAG, fh)
            path = fh.name
        try:
            args = _make_args(file=path, start="task_a", exit_code=True)
            assert trace_command(args) == 0
        finally:
            os.unlink(path)
