"""Tests for pipecheck.pruner."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.pruner import DAGPruner, PruneError, PruneResult


def _build_dag() -> DAG:
    """Build a small DAG: extract -> transform -> load, plus an orphan audit."""
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="extract", name="Extract"))
    dag.add_task(Task(task_id="transform", name="Transform", dependencies=["extract"]))
    dag.add_task(Task(task_id="load", name="Load", dependencies=["transform"]))
    dag.add_task(Task(task_id="audit", name="Audit"))  # not connected to load chain
    return dag


class TestPruneResult:
    def test_str_nothing_removed(self):
        dag = DAG(name="x")
        result = PruneResult(dag=dag, removed_tasks=[], kept_tasks=["a"])
        assert "kept=1" in str(result)
        assert "removed=0" in str(result)

    def test_str_with_removals(self):
        dag = DAG(name="x")
        result = PruneResult(dag=dag, removed_tasks=["audit"], kept_tasks=["extract"])
        text = str(result)
        assert "removed: audit" in text

    def test_has_removals_false(self):
        result = PruneResult(dag=DAG(name="x"), removed_tasks=[], kept_tasks=[])
        assert result.has_removals is False

    def test_has_removals_true(self):
        result = PruneResult(dag=DAG(name="x"), removed_tasks=["t1"], kept_tasks=[])
        assert result.has_removals is True


class TestDAGPruner:
    def test_prune_keeps_full_chain(self):
        dag = _build_dag()
        result = DAGPruner(dag).prune(["load"])
        kept_ids = {t.task_id for t in result.dag.tasks}
        assert kept_ids == {"extract", "transform", "load"}

    def test_prune_removes_unreachable(self):
        dag = _build_dag()
        result = DAGPruner(dag).prune(["load"])
        assert "audit" in result.removed_tasks

    def test_prune_single_root_no_deps(self):
        dag = _build_dag()
        result = DAGPruner(dag).prune(["extract"])
        kept_ids = {t.task_id for t in result.dag.tasks}
        assert kept_ids == {"extract"}
        assert len(result.removed_tasks) == 3

    def test_prune_multiple_roots(self):
        dag = _build_dag()
        result = DAGPruner(dag).prune(["load", "audit"])
        kept_ids = {t.task_id for t in result.dag.tasks}
        assert kept_ids == {"extract", "transform", "load", "audit"}
        assert result.removed_tasks == []

    def test_prune_preserves_dag_name(self):
        dag = _build_dag()
        result = DAGPruner(dag).prune(["load"])
        assert result.dag.name == "pipeline"

    def test_prune_empty_roots_raises(self):
        dag = _build_dag()
        with pytest.raises(PruneError, match="At least one root"):
            DAGPruner(dag).prune([])

    def test_prune_unknown_root_raises(self):
        dag = _build_dag()
        with pytest.raises(PruneError, match="not found"):
            DAGPruner(dag).prune(["nonexistent"])

    def test_kept_tasks_sorted(self):
        dag = _build_dag()
        result = DAGPruner(dag).prune(["load"])
        assert result.kept_tasks == sorted(result.kept_tasks)

    def test_removed_tasks_sorted(self):
        dag = _build_dag()
        result = DAGPruner(dag).prune(["extract"])
        assert result.removed_tasks == sorted(result.removed_tasks)
