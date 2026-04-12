"""Tests for pipecheck.marker."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.marker import (
    DAGMarker,
    MarkerEntry,
    MarkerError,
    MarkerResult,
    VALID_MARKERS,
)


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="extract", name="Extract"))
    dag.add_task(Task(task_id="transform", name="Transform"))
    dag.add_task(Task(task_id="load", name="Load"))
    return dag


class TestMarkerEntry:
    def test_str_without_reason(self):
        entry = MarkerEntry(task_id="extract", marker="skip")
        assert str(entry) == "[SKIP] extract"

    def test_str_with_reason(self):
        entry = MarkerEntry(task_id="load", marker="deprecated", reason="use load_v2")
        assert "[DEPRECATED]" in str(entry)
        assert "use load_v2" in str(entry)


class TestMarkerResult:
    def test_has_markers_false_when_empty(self):
        result = MarkerResult(dag_name="dag")
        assert result.has_markers is False

    def test_has_markers_true(self):
        result = MarkerResult(dag_name="dag")
        result.entries.append(MarkerEntry(task_id="t", marker="skip"))
        assert result.has_markers is True

    def test_count_property(self):
        result = MarkerResult(dag_name="dag")
        result.entries = [
            MarkerEntry(task_id="a", marker="skip"),
            MarkerEntry(task_id="b", marker="force"),
        ]
        assert result.count == 2

    def test_by_marker_filters_correctly(self):
        result = MarkerResult(dag_name="dag")
        result.entries = [
            MarkerEntry(task_id="a", marker="skip"),
            MarkerEntry(task_id="b", marker="force"),
            MarkerEntry(task_id="c", marker="skip"),
        ]
        skipped = result.by_marker("skip")
        assert len(skipped) == 2
        assert all(e.marker == "skip" for e in skipped)

    def test_str_no_markers(self):
        result = MarkerResult(dag_name="my_dag")
        assert "no markers" in str(result)

    def test_str_with_markers(self):
        result = MarkerResult(dag_name="my_dag")
        result.entries.append(MarkerEntry(task_id="extract", marker="skip"))
        assert "my_dag" in str(result)
        assert "SKIP" in str(result)


class TestDAGMarker:
    def test_mark_valid_task_and_marker(self):
        dag = _build_dag()
        m = DAGMarker(dag)
        entry = m.mark("extract", "skip")
        assert entry.task_id == "extract"
        assert entry.marker == "skip"

    def test_mark_with_reason(self):
        dag = _build_dag()
        m = DAGMarker(dag)
        entry = m.mark("load", "deprecated", reason="replaced")
        assert entry.reason == "replaced"

    def test_mark_invalid_marker_raises(self):
        dag = _build_dag()
        m = DAGMarker(dag)
        with pytest.raises(MarkerError, match="Unknown marker"):
            m.mark("extract", "unknown_marker")

    def test_mark_missing_task_raises(self):
        dag = _build_dag()
        m = DAGMarker(dag)
        with pytest.raises(MarkerError, match="not found"):
            m.mark("nonexistent", "skip")

    def test_unmark_removes_entry(self):
        dag = _build_dag()
        m = DAGMarker(dag)
        m.mark("extract", "force")
        m.unmark("extract")
        result = m.build()
        assert result.count == 0

    def test_unmark_missing_raises(self):
        dag = _build_dag()
        m = DAGMarker(dag)
        with pytest.raises(MarkerError, match="no marker"):
            m.unmark("extract")

    def test_build_returns_all_entries(self):
        dag = _build_dag()
        m = DAGMarker(dag)
        m.mark("extract", "skip")
        m.mark("transform", "experimental")
        result = m.build()
        assert result.dag_name == "test_dag"
        assert result.count == 2

    def test_valid_markers_set(self):
        assert "skip" in VALID_MARKERS
        assert "force" in VALID_MARKERS
        assert "deprecated" in VALID_MARKERS
        assert "experimental" in VALID_MARKERS
