"""Tests for pipecheck.tracer_path."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.tracer_path import (
    CriticalPathTracer,
    CriticalPath,
    PathStep,
    CriticalPathError,
)


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, task_id: str, timeout: float = 0.0) -> Task:
    task = Task(id=task_id, metadata={"timeout": timeout})
    dag.add_task(task)
    return task


def _edge(dag: DAG, src: str, dst: str) -> None:
    dag.add_edge(src, dst)


# ---------------------------------------------------------------------------
# PathStep
# ---------------------------------------------------------------------------

class TestPathStep:
    def test_str_no_weight(self):
        step = PathStep(task_id="a", weight=0.0, cumulative=0.0)
        result = str(step)
        assert "a" in result
        assert "weight=0.00" in result

    def test_str_with_weight(self):
        step = PathStep(task_id="heavy", weight=10.0, cumulative=10.0)
        result = str(step)
        assert "heavy" in result
        assert "weight=10.00" in result
        assert "[*]" in result


# ---------------------------------------------------------------------------
# CriticalPath
# ---------------------------------------------------------------------------

class TestCriticalPath:
    def test_task_ids_property(self):
        cp = CriticalPath(dag_name="d", steps=[
            PathStep("a", 1.0, 1.0),
            PathStep("b", 2.0, 3.0),
        ], total_weight=3.0)
        assert cp.task_ids == ["a", "b"]

    def test_length(self):
        cp = CriticalPath(dag_name="d", steps=[
            PathStep("a", 1.0, 1.0),
        ], total_weight=1.0)
        assert cp.length() == 1

    def test_str_contains_dag_name(self):
        cp = CriticalPath(dag_name="mypipe")
        assert "mypipe" in str(cp)

    def test_str_empty(self):
        cp = CriticalPath(dag_name="d")
        assert "empty" in str(cp)


# ---------------------------------------------------------------------------
# CriticalPathTracer
# ---------------------------------------------------------------------------

class TestCriticalPathTracer:
    def test_empty_dag_returns_empty_path(self):
        dag = _build_dag()
        tracer = CriticalPathTracer()
        result = tracer.trace(dag)
        assert result.length() == 0
        assert result.total_weight == 0.0

    def test_single_task(self):
        dag = _build_dag()
        _add(dag, "a", timeout=5.0)
        tracer = CriticalPathTracer()
        result = tracer.trace(dag)
        assert result.task_ids == ["a"]
        assert result.total_weight == pytest.approx(5.0)

    def test_chain_picks_only_path(self):
        dag = _build_dag()
        _add(dag, "a", timeout=1.0)
        _add(dag, "b", timeout=2.0)
        _add(dag, "c", timeout=3.0)
        _edge(dag, "a", "b")
        _edge(dag, "b", "c")
        tracer = CriticalPathTracer()
        result = tracer.trace(dag)
        assert result.task_ids == ["a", "b", "c"]
        assert result.total_weight == pytest.approx(6.0)

    def test_diamond_picks_heavier_branch(self):
        dag = _build_dag()
        _add(dag, "start", timeout=1.0)
        _add(dag, "light", timeout=1.0)
        _add(dag, "heavy", timeout=10.0)
        _add(dag, "end", timeout=1.0)
        _edge(dag, "start", "light")
        _edge(dag, "start", "heavy")
        _edge(dag, "light", "end")
        _edge(dag, "heavy", "end")
        tracer = CriticalPathTracer()
        result = tracer.trace(dag)
        assert "heavy" in result.task_ids
        assert "light" not in result.task_ids
        assert result.total_weight == pytest.approx(12.0)

    def test_custom_weight_attr(self):
        dag = _build_dag()
        t = Task(id="x", metadata={"cost": 7.0})
        dag.add_task(t)
        tracer = CriticalPathTracer()
        result = tracer.trace(dag, weight_attr="cost")
        assert result.total_weight == pytest.approx(7.0)

    def test_missing_weight_attr_defaults_to_zero(self):
        dag = _build_dag()
        _add(dag, "a")  # no timeout
        tracer = CriticalPathTracer()
        result = tracer.trace(dag)
        assert result.total_weight == pytest.approx(0.0)
