"""Tests for pipecheck.mapper."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.mapper import DAGMapper, MapError, MapResult, TaskMap


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    for tid in ("a", "b", "c", "d"):
        dag.add_task(Task(task_id=tid))
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


class TestTaskMap:
    def test_str_no_deps(self):
        tm = TaskMap(task_id="standalone")
        result = str(tm)
        assert "standalone" in result
        assert "none" in result

    def test_str_with_values(self):
        tm = TaskMap(task_id="mid", upstream=["a"], downstream=["z"])
        result = str(tm)
        assert "upstream=[a]" in result
        assert "downstream=[z]" in result


class TestMapResult:
    def test_len_reflects_entry_count(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        assert len(result) == 4

    def test_get_existing_task(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        entry = result.get("a")
        assert isinstance(entry, TaskMap)
        assert entry.task_id == "a"

    def test_get_missing_task_raises(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        with pytest.raises(MapError):
            result.get("nonexistent")

    def test_str_contains_dag_name(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        assert "test_dag" in str(result)

    def test_str_contains_all_task_ids(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        text = str(result)
        for tid in ("a", "b", "c", "d"):
            assert tid in text


class TestDAGMapper:
    def test_root_has_no_upstream(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        assert result.get("a").upstream == []

    def test_root_has_two_downstream(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        assert sorted(result.get("a").downstream) == ["b", "c"]

    def test_leaf_has_no_downstream(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        assert result.get("d").downstream == []

    def test_leaf_has_two_upstream(self):
        dag = _build_dag()
        result = DAGMapper().map(dag)
        assert sorted(result.get("d").upstream) == ["b", "c"]

    def test_empty_dag_returns_empty_result(self):
        dag = DAG(name="empty")
        result = DAGMapper().map(dag)
        assert len(result) == 0
        assert result.dag_name == "empty"
