"""Tests for pipecheck.batcher."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.batcher import Batch, BatchResult, DAGBatcher, BatchError


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, task_id: str, deps=None) -> None:
    dag.add_task(Task(task_id=task_id, dependencies=deps or []))


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

class TestBatch:
    def test_str_format(self):
        b = Batch(index=0, task_ids=["a", "b"])
        assert str(b) == "Batch 0: [a, b]"

    def test_str_empty(self):
        b = Batch(index=1)
        assert "(empty)" in str(b)

    def test_len(self):
        b = Batch(index=0, task_ids=["x", "y", "z"])
        assert len(b) == 3


# ---------------------------------------------------------------------------
# BatchResult
# ---------------------------------------------------------------------------

class TestBatchResult:
    def test_count_property(self):
        r = BatchResult(dag_name="d", batches=[Batch(0, ["a"]), Batch(1, ["b"])])
        assert r.count == 2

    def test_total_tasks(self):
        r = BatchResult(dag_name="d", batches=[Batch(0, ["a", "b"]), Batch(1, ["c"])])
        assert r.total_tasks == 3

    def test_str_contains_dag_name(self):
        r = BatchResult(dag_name="mypipe")
        assert "mypipe" in str(r)


# ---------------------------------------------------------------------------
# DAGBatcher
# ---------------------------------------------------------------------------

class TestDAGBatcher:
    def test_empty_dag_returns_empty_result(self):
        dag = _build_dag()
        result = DAGBatcher(dag).batch()
        assert result.count == 0
        assert result.total_tasks == 0

    def test_single_task_one_batch(self):
        dag = _build_dag()
        _add(dag, "a")
        result = DAGBatcher(dag).batch()
        assert result.count == 1
        assert result.batches[0].task_ids == ["a"]

    def test_chain_produces_sequential_batches(self):
        dag = _build_dag()
        _add(dag, "a")
        _add(dag, "b", ["a"])
        _add(dag, "c", ["b"])
        result = DAGBatcher(dag).batch()
        assert result.count == 3
        assert result.batches[0].task_ids == ["a"]
        assert result.batches[1].task_ids == ["b"]
        assert result.batches[2].task_ids == ["c"]

    def test_diamond_merges_parallel_tasks(self):
        dag = _build_dag()
        _add(dag, "root")
        _add(dag, "left", ["root"])
        _add(dag, "right", ["root"])
        _add(dag, "end", ["left", "right"])
        result = DAGBatcher(dag).batch()
        assert result.count == 3
        assert result.batches[0].task_ids == ["root"]
        assert sorted(result.batches[1].task_ids) == ["left", "right"]
        assert result.batches[2].task_ids == ["end"]

    def test_unknown_dependency_raises(self):
        dag = _build_dag()
        _add(dag, "a", ["ghost"])
        with pytest.raises(BatchError, match="Unknown dependency"):
            DAGBatcher(dag).batch()
