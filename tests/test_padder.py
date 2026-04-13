"""Tests for pipecheck.padder."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.padder import DAGPadder, PadEntry, PadError, PadResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    for tid in ("a", "b", "c", "d"):
        dag.add_task(Task(task_id=tid))
    # chain: a -> b -> c -> d
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    dag.add_edge("c", "d")
    return dag


class TestPadEntry:
    def test_str_format(self):
        entry = PadEntry(task_id="_pad_1", inserted_between=("a", "c"))
        s = str(entry)
        assert "_pad_1" in s
        assert "'a'" in s
        assert "'c'" in s


class TestPadResult:
    def test_has_pads_false_when_empty(self):
        r = PadResult(dag_name="x")
        assert r.has_pads() is False

    def test_has_pads_true_when_populated(self):
        r = PadResult(dag_name="x", entries=[PadEntry("_pad_1", ("a", "b"))])
        assert r.has_pads() is True

    def test_count_property(self):
        r = PadResult(
            dag_name="x",
            entries=[PadEntry("_pad_1", ("a", "b")), PadEntry("_pad_2", ("b", "c"))],
        )
        assert r.count() == 2

    def test_str_no_pads(self):
        r = PadResult(dag_name="mypipe")
        s = str(r)
        assert "no pads" in s
        assert "mypipe" in s

    def test_str_with_pads(self):
        r = PadResult(
            dag_name="mypipe",
            entries=[PadEntry("_pad_1", ("a", "c"))],
        )
        s = str(r)
        assert "1 pad" in s
        assert "_pad_1" in s


class TestDAGPadder:
    def test_invalid_max_depth_raises(self):
        dag = _build_dag()
        with pytest.raises(PadError):
            DAGPadder(dag, max_depth=0)

    def test_no_pads_needed_for_chain_depth_one(self):
        """A simple chain a->b->c->d with max_depth=1 needs no pads
        because each edge spans exactly one level."""
        dag = _build_dag()
        padder = DAGPadder(dag, max_depth=1)
        result = padder.pad()
        assert isinstance(result, PadResult)
        assert result.dag_name == "test"
        assert result.has_pads() is False

    def test_pad_inserted_for_long_edge(self):
        """Direct edge spanning 2 levels with max_depth=1 should get a pad."""
        dag = DAG(name="gapped")
        for tid in ("start", "middle", "end"):
            dag.add_task(Task(task_id=tid))
        dag.add_edge("start", "middle")
        dag.add_edge("middle", "end")
        # Add a long edge skipping middle: start -> end (gap=2)
        dag.add_edge("start", "end")
        padder = DAGPadder(dag, max_depth=1)
        result = padder.pad()
        # The start->end edge spans 2 levels, so one pad should be inserted
        assert result.has_pads()
        assert result.count() >= 1

    def test_padded_dag_is_returned(self):
        dag = _build_dag()
        padder = DAGPadder(dag, max_depth=2)
        result = padder.pad()
        assert result.padded_dag is not None
        assert isinstance(result.padded_dag, DAG)

    def test_padded_dag_contains_original_tasks(self):
        dag = _build_dag()
        padder = DAGPadder(dag, max_depth=2)
        result = padder.pad()
        for tid in ("a", "b", "c", "d"):
            assert tid in result.padded_dag.tasks
