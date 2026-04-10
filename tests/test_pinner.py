"""Tests for pipecheck.pinner and pipecheck.commands.pin_cmd."""
from __future__ import annotations

import json
import textwrap
from argparse import ArgumentParser
from pathlib import Path
from unittest.mock import patch

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.pinner import DAGPin, DAGPinner, PinnedTask


def _build_dag(name: str = "pipe") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task("extract", metadata={"timeout": 30, "retries": 2, "tags": ["io"]}))
    dag.add_task(Task("transform", metadata={"timeout": 60, "retries": 1}))
    dag.add_task(Task("load", metadata={"retries": 0}))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestPinnedTask:
    def test_str_representation(self):
        pt = PinnedTask(task_id="t1", timeout=10, retries=3)
        assert "t1" in str(pt)
        assert "timeout=10" in str(pt)
        assert "retries=3" in str(pt)

    def test_to_dict_keys(self):
        pt = PinnedTask(task_id="t1", timeout=None, retries=1, tags=["a", "b"])
        d = pt.to_dict()
        assert d["task_id"] == "t1"
        assert d["timeout"] is None
        assert d["retries"] == 1
        assert d["tags"] == ["a", "b"]

    def test_tags_sorted_in_dict(self):
        pt = PinnedTask(task_id="x", timeout=5, retries=0, tags=["z", "a", "m"])
        assert pt.to_dict()["tags"] == ["a", "m", "z"]


class TestDAGPinner:
    def test_pin_captures_task_ids(self):
        dag = _build_dag()
        pinner = DAGPinner()
        pin = pinner.pin(dag)
        assert set(pin.task_ids()) == {"extract", "transform", "load"}

    def test_pin_captures_timeout(self):
        dag = _build_dag()
        pin = DAGPinner().pin(dag)
        assert pin.tasks["extract"].timeout == 30

    def test_pin_captures_retries(self):
        dag = _build_dag()
        pin = DAGPinner().pin(dag)
        assert pin.tasks["transform"].retries == 1

    def test_pin_captures_tags(self):
        dag = _build_dag()
        pin = DAGPinner().pin(dag)
        assert "io" in pin.tasks["extract"].tags

    def test_pin_dag_name(self):
        dag = _build_dag(name="my_pipe")
        pin = DAGPinner().pin(dag)
        assert pin.dag_name == "my_pipe"

    def test_to_dict_structure(self):
        dag = _build_dag()
        pin = DAGPinner().pin(dag)
        d = pin.to_dict()
        assert "dag_name" in d
        assert "tasks" in d
        assert "extract" in d["tasks"]

    def test_diff_no_changes(self):
        dag = _build_dag()
        pinner = DAGPinner()
        pin = pinner.pin(dag)
        assert pinner.diff_pins(pin, pin) == []

    def test_diff_detects_added_task(self):
        dag = _build_dag()
        pinner = DAGPinner()
        old_pin = pinner.pin(dag)
        dag.add_task(Task("validate"))
        new_pin = pinner.pin(dag)
        changes = pinner.diff_pins(old_pin, new_pin)
        assert any("added task" in c and "validate" in c for c in changes)

    def test_diff_detects_removed_task(self):
        dag = _build_dag()
        pinner = DAGPinner()
        old_pin = pinner.pin(dag)
        new_dag = DAG(name="pipe")
        new_dag.add_task(Task("extract"))
        new_pin = pinner.pin(new_dag)
        changes = pinner.diff_pins(old_pin, new_pin)
        assert any("removed task" in c for c in changes)

    def test_diff_detects_timeout_change(self):
        dag = _build_dag()
        pinner = DAGPinner()
        old_pin = pinner.pin(dag)
        dag.tasks["extract"].metadata["timeout"] = 99
        new_pin = pinner.pin(dag)
        changes = pinner.diff_pins(old_pin, new_pin)
        assert any("timeout" in c and "extract" in c for c in changes)

    def test_diff_detects_retries_change(self):
        dag = _build_dag()
        pinner = DAGPinner()
        old_pin = pinner.pin(dag)
        dag.tasks["transform"].metadata["retries"] = 5
        new_pin = pinner.pin(dag)
        changes = pinner.diff_pins(old_pin, new_pin)
        assert any("retries" in c and "transform" in c for c in changes)
