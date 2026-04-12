"""Tests for pipecheck.reducer."""

import pytest
from pipecheck.dag import DAG, Task
from pipecheck.reducer import DAGReducer, ReduceError, ReduceEntry, ReduceResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    for tid in ["a", "b", "c", "d"]:
        dag.add_task(Task(task_id=tid))
    return dag


# ---------------------------------------------------------------------------
# ReduceEntry
# ---------------------------------------------------------------------------

class TestReduceEntry:
    def test_str_format(self):
        entry = ReduceEntry(original_ids=["a", "b", "c"], compound_id="a__b__c")
        assert str(entry) == "a -> b -> c => a__b__c"

    def test_str_two_tasks(self):
        entry = ReduceEntry(original_ids=["x", "y"], compound_id="x__y")
        assert "x -> y" in str(entry)
        assert "x__y" in str(entry)


# ---------------------------------------------------------------------------
# ReduceResult
# ---------------------------------------------------------------------------

class TestReduceResult:
    def _make_dag(self):
        return DAG(name="r")

    def test_has_reductions_false_when_empty(self):
        result = ReduceResult(dag=self._make_dag())
        assert result.has_reductions is False

    def test_has_reductions_true(self):
        entry = ReduceEntry(original_ids=["a", "b"], compound_id="a__b")
        result = ReduceResult(dag=self._make_dag(), entries=[entry])
        assert result.has_reductions is True

    def test_count_property(self):
        entries = [
            ReduceEntry(["a", "b"], "a__b"),
            ReduceEntry(["c", "d"], "c__d"),
        ]
        result = ReduceResult(dag=self._make_dag(), entries=entries)
        assert result.count == 2

    def test_str_no_reductions(self):
        result = ReduceResult(dag=self._make_dag())
        assert "no reductions" in str(result)

    def test_str_with_reductions(self):
        entry = ReduceEntry(original_ids=["a", "b"], compound_id="a__b")
        result = ReduceResult(dag=self._make_dag(), entries=[entry])
        text = str(result)
        assert "1 chain(s)" in text
        assert "a__b" in text


# ---------------------------------------------------------------------------
# DAGReducer
# ---------------------------------------------------------------------------

class TestDAGReducer:
    def test_empty_dag_raises(self):
        dag = DAG(name="empty")
        with pytest.raises(ReduceError):
            DAGReducer().reduce(dag)

    def test_no_chain_no_reductions(self):
        dag = _build_dag()
        # a->b, a->c, b->d, c->d  (diamond — no linear chain)
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        result = DAGReducer().reduce(dag)
        assert result.has_reductions is False

    def test_simple_chain_collapsed(self):
        dag = DAG(name="chain")
        for tid in ["a", "b", "c"]:
            dag.add_task(Task(task_id=tid))
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        result = DAGReducer().reduce(dag)
        assert result.has_reductions is True
        assert result.count == 1
        assert result.entries[0].compound_id == "a__b__c"

    def test_reduced_dag_preserves_name(self):
        dag = DAG(name="mydag")
        for tid in ["x", "y"]:
            dag.add_task(Task(task_id=tid))
        dag.add_edge("x", "y")
        result = DAGReducer().reduce(dag)
        assert result.dag.name == "mydag"

    def test_compound_task_in_reduced_dag(self):
        dag = DAG(name="pipe")
        for tid in ["p", "q"]:
            dag.add_task(Task(task_id=tid))
        dag.add_edge("p", "q")
        result = DAGReducer().reduce(dag)
        assert "p__q" in result.dag.tasks

    def test_timeout_takes_max_of_chain(self):
        dag = DAG(name="t")
        dag.add_task(Task(task_id="a", timeout=10))
        dag.add_task(Task(task_id="b", timeout=30))
        dag.add_edge("a", "b")
        result = DAGReducer().reduce(dag)
        compound = result.dag.tasks["a__b"]
        assert compound.timeout == 30
