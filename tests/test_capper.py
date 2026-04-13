"""Tests for pipecheck.capper."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.capper import CapError, CapResult, DAGCapper


def _build_dag(name: str, task_ids) -> DAG:
    dag = DAG(name=name)
    for tid in task_ids:
        dag.add_task(Task(id=tid, name=tid))
    return dag


class TestCapResult:
    def test_str_ok(self):
        result = CapResult(dag_name="pipe", limit=5, actual=3, exceeded=False, over_by=0)
        text = str(result)
        assert "pipe" in text
        assert "OK" in text
        assert "EXCEEDED" not in text

    def test_str_exceeded(self):
        result = CapResult(
            dag_name="pipe",
            limit=2,
            actual=4,
            exceeded=True,
            over_by=2,
            offending_tasks=["task_c", "task_d"],
        )
        text = str(result)
        assert "EXCEEDED" in text
        assert "Over by: 2" in text
        assert "task_c" in text
        assert "task_d" in text

    def test_str_exceeded_no_offending_tasks(self):
        result = CapResult(
            dag_name="pipe", limit=1, actual=2, exceeded=True, over_by=1, offending_tasks=[]
        )
        text = str(result)
        assert "Extra tasks" not in text


class TestDAGCapper:
    def setup_method(self):
        self.capper = DAGCapper()

    def test_within_limit_not_exceeded(self):
        dag = _build_dag("my_dag", ["a", "b", "c"])
        result = self.capper.cap(dag, limit=5)
        assert not result.exceeded
        assert result.actual == 3
        assert result.over_by == 0
        assert result.offending_tasks == []

    def test_exactly_at_limit_not_exceeded(self):
        dag = _build_dag("my_dag", ["a", "b"])
        result = self.capper.cap(dag, limit=2)
        assert not result.exceeded
        assert result.over_by == 0

    def test_exceeds_limit(self):
        dag = _build_dag("my_dag", ["a", "b", "c", "d"])
        result = self.capper.cap(dag, limit=2)
        assert result.exceeded
        assert result.over_by == 2
        assert len(result.offending_tasks) == 2

    def test_dag_name_stored(self):
        dag = _build_dag("etl_pipeline", ["a"])
        result = self.capper.cap(dag, limit=10)
        assert result.dag_name == "etl_pipeline"

    def test_zero_limit_with_empty_dag(self):
        dag = _build_dag("empty", [])
        result = self.capper.cap(dag, limit=0)
        assert not result.exceeded

    def test_zero_limit_with_tasks_exceeded(self):
        dag = _build_dag("small", ["a"])
        result = self.capper.cap(dag, limit=0)
        assert result.exceeded
        assert result.over_by == 1

    def test_negative_limit_raises(self):
        dag = _build_dag("x", ["a"])
        with pytest.raises(CapError):
            self.capper.cap(dag, limit=-1)
