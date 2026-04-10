"""Tests for pipecheck.trimmer."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.trimmer import DAGTrimmer, TrimError, TrimResult


def _build_dag(spec: dict) -> DAG:
    """Helper: spec maps task_id -> list of dependency ids."""
    dag = DAG(name="test_dag")
    for tid, deps in spec.items():
        dag.add_task(Task(task_id=tid, dependencies=deps))
    return dag


# ---------------------------------------------------------------------------
# TrimResult
# ---------------------------------------------------------------------------

class TestTrimResult:
    def test_str_nothing_removed(self):
        dag = _build_dag({"a": []})
        result = TrimResult(original_dag=dag, trimmed_dag=dag, removed_task_ids=[])
        assert "nothing to trim" in str(result)

    def test_str_with_removals(self):
        dag = _build_dag({"a": [], "b": []})
        trimmed = _build_dag({"a": []})
        result = TrimResult(original_dag=dag, trimmed_dag=trimmed, removed_task_ids=["b"])
        assert "removed 1 task(s)" in str(result)
        assert "b" in str(result)

    def test_has_removals_false(self):
        dag = _build_dag({"a": []})
        result = TrimResult(original_dag=dag, trimmed_dag=dag, removed_task_ids=[])
        assert result.has_removals is False

    def test_has_removals_true(self):
        dag = _build_dag({"a": [], "b": []})
        result = TrimResult(original_dag=dag, trimmed_dag=dag, removed_task_ids=["b"])
        assert result.has_removals is True


# ---------------------------------------------------------------------------
# DAGTrimmer.trim_unreachable
# ---------------------------------------------------------------------------

class TestTrimUnreachable:
    def setup_method(self):
        self.trimmer = DAGTrimmer()

    def test_all_reachable_no_removals(self):
        dag = _build_dag({"a": [], "b": ["a"], "c": ["b"]})
        result = self.trimmer.trim_unreachable(dag)
        assert not result.has_removals
        assert set(result.trimmed_dag.tasks.keys()) == {"a", "b", "c"}

    def test_raises_when_no_roots(self):
        # Cyclic graph has no roots
        dag = _build_dag({"a": ["b"], "b": ["a"]})
        with pytest.raises(TrimError):
            self.trimmer.trim_unreachable(dag)

    def test_downstream_tasks_kept(self):
        dag = _build_dag({"root": [], "mid": ["root"], "leaf": ["mid"]})
        result = self.trimmer.trim_unreachable(dag)
        assert set(result.trimmed_dag.tasks.keys()) == {"root", "mid", "leaf"}


# ---------------------------------------------------------------------------
# DAGTrimmer.trim_isolated
# ---------------------------------------------------------------------------

class TestTrimIsolated:
    def setup_method(self):
        self.trimmer = DAGTrimmer()

    def test_removes_isolated_task(self):
        # 'orphan' has no deps and nothing depends on it
        dag = _build_dag({"a": [], "b": ["a"], "orphan": []})
        result = self.trimmer.trim_isolated(dag)
        assert "orphan" in result.removed_task_ids
        assert "a" not in result.removed_task_ids

    def test_single_task_dag_not_removed(self):
        dag = _build_dag({"solo": []})
        result = self.trimmer.trim_isolated(dag)
        assert not result.has_removals

    def test_connected_dag_no_removals(self):
        dag = _build_dag({"a": [], "b": ["a"]})
        result = self.trimmer.trim_isolated(dag)
        assert not result.has_removals

    def test_trimmed_dag_preserves_edges(self):
        dag = _build_dag({"a": [], "b": ["a"], "c": ["b"], "iso": []})
        result = self.trimmer.trim_isolated(dag)
        assert result.trimmed_dag.get_task("b").dependencies == ["a"]
        assert "iso" not in result.trimmed_dag.tasks
