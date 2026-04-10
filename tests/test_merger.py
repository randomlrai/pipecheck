"""Tests for pipecheck.merger."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.merger import DAGMerger, MergeConflict, MergeError, MergeResult


def _simple_dag(name: str, task_ids: list) -> DAG:
    dag = DAG(name=name)
    for tid in task_ids:
        dag.add_task(Task(task_id=tid, command=f"run_{tid}"))
    return dag


class TestMergeConflict:
    def test_str_format(self):
        c = MergeConflict(task_id="load", reason="duplicate")
        assert "load" in str(c)
        assert "duplicate" in str(c)


class TestMergeResult:
    def test_has_conflicts_false_when_empty(self):
        dag = DAG(name="merged")
        result = MergeResult(dag=dag)
        assert result.has_conflicts is False

    def test_has_conflicts_true_when_present(self):
        dag = DAG(name="merged")
        result = MergeResult(dag=dag, conflicts=[MergeConflict("x", "dup")])
        assert result.has_conflicts is True

    def test_str_contains_dag_name(self):
        dag = DAG(name="pipeline")
        result = MergeResult(dag=dag)
        assert "pipeline" in str(result)

    def test_str_mentions_no_conflicts(self):
        dag = DAG(name="pipeline")
        result = MergeResult(dag=dag)
        assert "No conflicts" in str(result)

    def test_str_lists_conflicts(self):
        dag = DAG(name="pipeline")
        result = MergeResult(dag=dag, conflicts=[MergeConflict("t1", "dup")])
        assert "conflict" in str(result).lower()
        assert "t1" in str(result)


class TestDAGMerger:
    def test_merge_distinct_dags(self):
        dag_a = _simple_dag("a", ["extract", "transform"])
        dag_b = _simple_dag("b", ["load"])
        merger = DAGMerger(merged_name="combined")
        result = merger.merge(dag_a, dag_b)
        assert result.dag.name == "combined"
        assert set(result.dag.tasks.keys()) == {"extract", "transform", "load"}
        assert not result.has_conflicts

    def test_merge_preserves_edges(self):
        dag_a = _simple_dag("a", ["t1", "t2"])
        dag_a.add_edge("t1", "t2")
        dag_b = _simple_dag("b", ["t3"])
        merger = DAGMerger()
        result = merger.merge(dag_a, dag_b)
        assert ("t1", "t2") in result.dag.edges

    def test_conflict_raises_by_default(self):
        dag_a = _simple_dag("a", ["shared"])
        dag_b = _simple_dag("b", ["shared"])
        merger = DAGMerger()
        with pytest.raises(MergeError):
            merger.merge(dag_a, dag_b)

    def test_conflict_skip_keeps_first(self):
        dag_a = _simple_dag("a", ["shared"])
        dag_b = _simple_dag("b", ["shared"])
        # Give dag_b's task a different command to distinguish
        dag_b.tasks["shared"].command = "run_b"
        merger = DAGMerger()
        result = merger.merge(dag_a, dag_b, on_conflict="skip")
        assert result.has_conflicts
        assert result.dag.tasks["shared"].command == "run_shared"

    def test_conflict_overwrite_uses_last(self):
        dag_a = _simple_dag("a", ["shared"])
        dag_b = _simple_dag("b", ["shared"])
        dag_b.tasks["shared"].command = "run_b"
        merger = DAGMerger()
        result = merger.merge(dag_a, dag_b, on_conflict="overwrite")
        assert result.has_conflicts
        assert result.dag.tasks["shared"].command == "run_b"

    def test_invalid_on_conflict_raises_value_error(self):
        dag_a = _simple_dag("a", ["t1"])
        merger = DAGMerger()
        with pytest.raises(ValueError):
            merger.merge(dag_a, on_conflict="invalid")

    def test_merge_single_dag(self):
        dag_a = _simple_dag("solo", ["only_task"])
        merger = DAGMerger(merged_name="result")
        result = merger.merge(dag_a)
        assert len(result.dag.tasks) == 1
        assert not result.has_conflicts
