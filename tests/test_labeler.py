"""Tests for pipecheck.labeler."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.labeler import DAGLabeler, LabelEntry, LabelError, LabelResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    for tid in ("ingest", "transform", "load"):
        dag.add_task(Task(task_id=tid, command=f"run_{tid}"))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "load")
    return dag


# ---------------------------------------------------------------------------
# LabelEntry
# ---------------------------------------------------------------------------

class TestLabelEntry:
    def test_str_without_color(self):
        e = LabelEntry(task_id="a", label="Alpha")
        assert str(e) == 'a -> "Alpha"'

    def test_str_with_color(self):
        e = LabelEntry(task_id="b", label="Beta", color="blue")
        assert str(e) == 'b -> "Beta" [blue]'


# ---------------------------------------------------------------------------
# LabelResult
# ---------------------------------------------------------------------------

class TestLabelResult:
    def test_has_labels_false_when_empty(self):
        r = LabelResult(dag_name="d")
        assert r.has_labels is False

    def test_has_labels_true(self):
        r = LabelResult(dag_name="d", entries=[LabelEntry("a", "A")])
        assert r.has_labels is True

    def test_count(self):
        r = LabelResult(
            dag_name="d",
            entries=[LabelEntry("a", "A"), LabelEntry("b", "B")],
        )
        assert r.count == 2

    def test_get_existing(self):
        e = LabelEntry("a", "Alpha")
        r = LabelResult(dag_name="d", entries=[e])
        assert r.get("a") is e

    def test_get_missing_returns_none(self):
        r = LabelResult(dag_name="d")
        assert r.get("x") is None

    def test_str_no_labels(self):
        r = LabelResult(dag_name="d")
        assert "no labels" in str(r)

    def test_str_with_labels(self):
        r = LabelResult(
            dag_name="mypipe",
            entries=[LabelEntry("a", "Alpha"), LabelEntry("b", "Beta")],
        )
        text = str(r)
        assert "mypipe" in text
        assert "2 label" in text
        assert "Alpha" in text


# ---------------------------------------------------------------------------
# DAGLabeler
# ---------------------------------------------------------------------------

class TestDAGLabeler:
    def test_labels_known_tasks(self):
        dag = _build_dag()
        result = DAGLabeler().label(dag, {"ingest": "Ingest Data", "load": "Load"})
        assert result.count == 2
        assert result.get("ingest").label == "Ingest Data"

    def test_entries_sorted_by_task_id(self):
        dag = _build_dag()
        result = DAGLabeler().label(
            dag, {"transform": "T", "ingest": "I", "load": "L"}
        )
        ids = [e.task_id for e in result.entries]
        assert ids == sorted(ids)

    def test_color_map_applied(self):
        dag = _build_dag()
        result = DAGLabeler().label(
            dag,
            {"ingest": "Ingest"},
            color_map={"ingest": "green"},
        )
        assert result.get("ingest").color == "green"

    def test_unknown_task_raises_label_error(self):
        dag = _build_dag()
        with pytest.raises(LabelError, match="unknown_task"):
            DAGLabeler().label(dag, {"unknown_task": "Ghost"})

    def test_empty_label_map_returns_empty_result(self):
        dag = _build_dag()
        result = DAGLabeler().label(dag, {})
        assert result.has_labels is False
        assert result.dag_name == "test_dag"
