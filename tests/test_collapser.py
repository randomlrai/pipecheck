"""Tests for pipecheck.collapser."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.collapser import CollapseEntry, CollapseResult, DAGCollapser, CollapseError


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, *ids: str) -> None:
    for tid in ids:
        dag.add_task(Task(id=tid))


def _edge(dag: DAG, src: str, dst: str) -> None:
    dag.add_edge(src, dst)


class TestCollapseEntry:
    def test_str_format(self):
        entry = CollapseEntry(original_ids=["a", "b", "c"], collapsed_id="a__b__c")
        result = str(entry)
        assert "a -> b -> c" in result
        assert "a__b__c" in result

    def test_str_two_tasks(self):
        entry = CollapseEntry(original_ids=["x", "y"], collapsed_id="x__y")
        assert "x -> y" in str(entry)
        assert "x__y" in str(entry)


class TestCollapseResult:
    def test_has_collapses_false_when_empty(self):
        r = CollapseResult(dag_name="d")
        assert r.has_collapses is False

    def test_has_collapses_true_when_populated(self):
        r = CollapseResult(dag_name="d")
        r.entries.append(CollapseEntry(original_ids=["a", "b"], collapsed_id="a__b"))
        assert r.has_collapses is True

    def test_count_property(self):
        r = CollapseResult(dag_name="d")
        r.entries.append(CollapseEntry(original_ids=["a", "b"], collapsed_id="a__b"))
        r.entries.append(CollapseEntry(original_ids=["c", "d"], collapsed_id="c__d"))
        assert r.count == 2

    def test_str_no_collapses(self):
        r = CollapseResult(dag_name="mypipe")
        assert "no chains collapsed" in str(r)
        assert "mypipe" in str(r)

    def test_str_with_collapses(self):
        r = CollapseResult(dag_name="mypipe")
        r.entries.append(CollapseEntry(original_ids=["a", "b"], collapsed_id="a__b"))
        s = str(r)
        assert "1 chain(s) collapsed" in s
        assert "a -> b" in s


class TestDAGCollapser:
    def test_no_chain_returns_same_tasks(self):
        dag = _build_dag()
        _add(dag, "a", "b", "c")
        _edge(dag, "a", "b")
        _edge(dag, "a", "c")
        collapser = DAGCollapser()
        result = collapser.collapse(dag)
        assert result.has_collapses is False
        assert result.dag is not None

    def test_linear_chain_collapsed(self):
        dag = _build_dag("pipe")
        _add(dag, "a", "b", "c")
        _edge(dag, "a", "b")
        _edge(dag, "b", "c")
        collapser = DAGCollapser()
        result = collapser.collapse(dag)
        assert result.has_collapses is True
        assert result.count == 1
        entry = result.entries[0]
        assert "a" in entry.original_ids
        assert "b" in entry.original_ids

    def test_collapsed_dag_has_fewer_nodes(self):
        dag = _build_dag("pipe")
        _add(dag, "a", "b", "c", "d")
        _edge(dag, "a", "b")
        _edge(dag, "b", "c")
        _edge(dag, "c", "d")
        collapser = DAGCollapser()
        result = collapser.collapse(dag)
        assert len(result.dag.tasks) < len(dag.tasks)

    def test_dag_name_preserved(self):
        dag = _build_dag("mydag")
        _add(dag, "x", "y")
        _edge(dag, "x", "y")
        result = DAGCollapser().collapse(dag)
        assert result.dag_name == "mydag"

    def test_single_task_no_collapse(self):
        dag = _build_dag()
        _add(dag, "solo")
        result = DAGCollapser().collapse(dag)
        assert result.has_collapses is False
        assert "solo" in result.dag.tasks
