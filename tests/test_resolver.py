"""Tests for pipecheck.resolver."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.resolver import DAGResolver, ResolveError, ResolveResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    for tid in ("a", "b", "c", "d"):
        dag.add_task(Task(task_id=tid))
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


class TestResolveResult:
    def test_str_non_empty(self):
        r = ResolveResult(dag_name="pipe", order=["a", "b", "c"])
        assert "a -> b -> c" in str(r)
        assert "pipe" in str(r)

    def test_str_empty(self):
        r = ResolveResult(dag_name="pipe")
        assert "empty" in str(r)

    def test_count_property(self):
        r = ResolveResult(dag_name="x", order=["a", "b"])
        assert r.count == 2

    def test_count_empty(self):
        r = ResolveResult(dag_name="x")
        assert r.count == 0


class TestDAGResolver:
    def test_resolve_diamond_contains_all_tasks(self):
        dag = _build_dag()
        result = DAGResolver(dag).resolve()
        assert set(result.order) == {"a", "b", "c", "d"}

    def test_resolve_diamond_respects_order(self):
        dag = _build_dag()
        result = DAGResolver(dag).resolve()
        assert result.order.index("a") < result.order.index("b")
        assert result.order.index("a") < result.order.index("c")
        assert result.order.index("b") < result.order.index("d")
        assert result.order.index("c") < result.order.index("d")

    def test_resolve_single_task(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only"))
        result = DAGResolver(dag).resolve()
        assert result.order == ["only"]

    def test_resolve_linear_chain(self):
        dag = DAG(name="chain")
        for tid in ("x", "y", "z"):
            dag.add_task(Task(task_id=tid))
        dag.add_edge("x", "y")
        dag.add_edge("y", "z")
        result = DAGResolver(dag).resolve()
        assert result.order == ["x", "y", "z"]

    def test_resolve_empty_dag(self):
        dag = DAG(name="empty")
        result = DAGResolver(dag).resolve()
        assert result.order == []

    def test_resolve_raises_on_cycle(self):
        dag = DAG(name="cyclic")
        for tid in ("p", "q"):
            dag.add_task(Task(task_id=tid))
        dag.add_edge("p", "q")
        dag.add_edge("q", "p")
        with pytest.raises(ResolveError, match="Cycle detected"):
            DAGResolver(dag).resolve()

    def test_result_dag_name_stored(self):
        dag = _build_dag(name="my_pipe")
        result = DAGResolver(dag).resolve()
        assert result.dag_name == "my_pipe"
