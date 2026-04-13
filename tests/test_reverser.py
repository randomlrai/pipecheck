"""Tests for pipecheck.reverser."""

from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.reverser import DAGReverser, ReverseError, ReverseResult


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="ingest"))
    dag.add_task(Task(task_id="transform", dependencies=["ingest"]))
    dag.add_task(Task(task_id="load", dependencies=["transform"]))
    return dag


# ---------------------------------------------------------------------------
# ReverseResult
# ---------------------------------------------------------------------------

class TestReverseResult:
    def test_str_format(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        text = str(result)
        assert "pipeline" in text
        assert "tasks=3" in text
        assert "edges=2" in text

    def test_count_property(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        assert result.count == 3

    def test_original_name_stored(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        assert result.original_name == "pipeline"

    def test_edge_count_stored(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        assert result.edge_count == 2


# ---------------------------------------------------------------------------
# DAGReverser
# ---------------------------------------------------------------------------

class TestDAGReverser:
    def test_reversed_dag_name_has_suffix(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        assert result.reversed_dag.name == "pipeline_reversed"

    def test_all_tasks_present(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        assert set(result.reversed_dag.tasks.keys()) == {"ingest", "transform", "load"}

    def test_edges_are_flipped(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        rev = result.reversed_dag
        # original: ingest -> transform -> load
        # reversed: ingest depends on transform, transform depends on load
        assert "transform" in rev.tasks["ingest"].dependencies
        assert "load" in rev.tasks["transform"].dependencies

    def test_original_roots_become_leaves(self):
        dag = _build_dag()
        result = DAGReverser().reverse(dag)
        rev = result.reversed_dag
        # "load" was a leaf; in reversed graph it should have no dependents
        assert rev.tasks["load"].dependencies == []

    def test_empty_dag_raises(self):
        empty = DAG(name="empty")
        with pytest.raises(ReverseError):
            DAGReverser().reverse(empty)

    def test_single_task_no_edges(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only"))
        result = DAGReverser().reverse(dag)
        assert result.edge_count == 0
        assert result.count == 1

    def test_metadata_copied(self):
        dag = DAG(name="meta")
        dag.add_task(Task(task_id="a", metadata={"owner": "alice"}))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        result = DAGReverser().reverse(dag)
        assert result.reversed_dag.tasks["a"].metadata["owner"] == "alice"
