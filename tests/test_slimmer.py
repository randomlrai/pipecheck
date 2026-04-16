"""Tests for pipecheck.slimmer."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.slimmer import DAGSlimmer, SlimEntry, SlimResult


def _build_dag(name="test_dag") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag, task_id):
    dag.add_task(Task(task_id=task_id))


def _edge(dag, src, dst):
    dag.add_edge(src, dst)


class TestSlimEntry:
    def test_str_format(self):
        e = SlimEntry(source="a", target="b")
        assert "a -> b" in str(e)
        assert "redundant" in str(e)


class TestSlimResult:
    def test_has_removals_false_when_empty(self):
        r = SlimResult(dag_name="d")
        assert not r.has_removals

    def test_has_removals_true_when_populated(self):
        r = SlimResult(dag_name="d", removed=[SlimEntry("a", "c")])
        assert r.has_removals

    def test_count_property(self):
        r = SlimResult(dag_name="d", removed=[SlimEntry("a", "c"), SlimEntry("b", "c")])
        assert r.count == 2

    def test_str_no_removals(self):
        r = SlimResult(dag_name="mypipe")
        s = str(r)
        assert "mypipe" in s
        assert "No redundant" in s

    def test_str_with_removals(self):
        r = SlimResult(dag_name="mypipe", removed=[SlimEntry("a", "c")])
        s = str(r)
        assert "a -> c" in s


class TestDAGSlimmer:
    def test_no_redundant_edges_in_simple_chain(self):
        dag = _build_dag()
        for t in ["a", "b", "c"]:
            _add(dag, t)
        _edge(dag, "a", "b")
        _edge(dag, "b", "c")
        result = DAGSlimmer().slim(dag)
        assert not result.has_removals

    def test_detects_redundant_transitive_edge(self):
        dag = _build_dag()
        for t in ["a", "b", "c"]:
            _add(dag, t)
        _edge(dag, "a", "b")
        _edge(dag, "b", "c")
        _edge(dag, "a", "c")  # redundant: a->b->c already covers a->c
        result = DAGSlimmer().slim(dag)
        assert result.has_removals
        pairs = [(e.source, e.target) for e in result.removed]
        assert ("a", "c") in pairs

    def test_diamond_no_redundant_edges(self):
        dag = _build_dag()
        for t in ["a", "b", "c", "d"]:
            _add(dag, t)
        _edge(dag, "a", "b")
        _edge(dag, "a", "c")
        _edge(dag, "b", "d")
        _edge(dag, "c", "d")
        result = DAGSlimmer().slim(dag)
        # a->d would be redundant if present, but it's not
        assert not result.has_removals

    def test_result_stores_dag_name(self):
        dag = _build_dag(name="pipeline_x")
        _add(dag, "a")
        result = DAGSlimmer().slim(dag)
        assert result.dag_name == "pipeline_x"
