"""Tests for pipecheck.pinpointer."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.pinpointer import DAGPinpointer, PinpointResult


def _build_dag(name="test_dag") -> DAG:
    dag = DAG(name=name)
    for tid in ("extract", "transform", "load"):
        dag.add_task(Task(task_id=tid))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestPinpointResult:
    def test_str_contains_dag_name(self):
        r = PinpointResult(dag_name="my_dag", roots=["a"], leaves=["b"])
        assert "my_dag" in str(r)

    def test_str_shows_roots(self):
        r = PinpointResult(dag_name="d", roots=["r1", "r2"])
        assert "r1" in str(r)
        assert "r2" in str(r)

    def test_str_shows_none_when_empty(self):
        r = PinpointResult(dag_name="d")
        text = str(r)
        assert "none" in text

    def test_count_sums_all_categories(self):
        r = PinpointResult(
            dag_name="d",
            roots=["a"],
            leaves=["c"],
            intermediates=["b"],
        )
        assert r.count == 3

    def test_has_intermediates_false_when_empty(self):
        r = PinpointResult(dag_name="d", roots=["a"], leaves=["a"])
        assert r.has_intermediates() is False

    def test_has_intermediates_true_when_populated(self):
        r = PinpointResult(dag_name="d", intermediates=["b"])
        assert r.has_intermediates() is True


class TestDAGPinpointer:
    def test_chain_root_leaf_intermediate(self):
        dag = _build_dag()
        result = DAGPinpointer(dag).pinpoint()
        assert result.roots == ["extract"]
        assert result.leaves == ["load"]
        assert result.intermediates == ["transform"]

    def test_single_task_is_both_root_and_leaf(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only"))
        result = DAGPinpointer(dag).pinpoint()
        assert "only" in result.roots
        assert "only" in result.leaves
        assert result.intermediates == []

    def test_empty_dag_returns_empty_result(self):
        dag = DAG(name="empty")
        result = DAGPinpointer(dag).pinpoint()
        assert result.count == 0
        assert result.dag_name == "empty"

    def test_diamond_has_one_root_one_leaf_two_intermediates(self):
        dag = DAG(name="diamond")
        for tid in ("a", "b", "c", "d"):
            dag.add_task(Task(task_id=tid))
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        result = DAGPinpointer(dag).pinpoint()
        assert result.roots == ["a"]
        assert result.leaves == ["d"]
        assert sorted(result.intermediates) == ["b", "c"]

    def test_dag_name_preserved(self):
        dag = _build_dag(name="pipeline_x")
        result = DAGPinpointer(dag).pinpoint()
        assert result.dag_name == "pipeline_x"
