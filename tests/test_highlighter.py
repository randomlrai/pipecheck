"""Tests for pipecheck.highlighter."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.highlighter import (
    DAGHighlighter,
    HighlightEntry,
    HighlightError,
    HighlightResult,
)


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task("extract", metadata={"tags": ["io", "source"]}))
    dag.add_task(Task("transform", metadata={"tags": ["compute"]}))
    dag.add_task(Task("load", metadata={"tags": ["io", "sink"]}))
    dag.add_task(Task("notify", metadata={"tags": []}))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "notify")
    return dag


class TestHighlightEntry:
    def test_str_format(self):
        entry = HighlightEntry(task_id="my_task", reason="tag=io", color="yellow")
        assert str(entry) == "[yellow] my_task: tag=io"

    def test_str_custom_color(self):
        entry = HighlightEntry(task_id="other", reason="manual", color="red")
        assert "[red]" in str(entry)
        assert "other" in str(entry)


class TestHighlightResult:
    def test_has_highlights_false_when_empty(self):
        result = HighlightResult(dag_name="dag")
        assert result.has_highlights() is False

    def test_has_highlights_true_when_populated(self):
        result = HighlightResult(dag_name="dag")
        result.entries.append(HighlightEntry("t", "x"))
        assert result.has_highlights() is True

    def test_count_property(self):
        result = HighlightResult(dag_name="dag")
        result.entries.append(HighlightEntry("a", "r"))
        result.entries.append(HighlightEntry("b", "r"))
        assert result.count() == 2

    def test_str_no_highlights(self):
        result = HighlightResult(dag_name="my_dag")
        assert "No highlights" in str(result)
        assert "my_dag" in str(result)

    def test_str_with_highlights(self):
        result = HighlightResult(dag_name="my_dag")
        result.entries.append(HighlightEntry("task_a", "tag=io", "yellow"))
        out = str(result)
        assert "my_dag" in out
        assert "task_a" in out


class TestDAGHighlighter:
    def setup_method(self):
        self._dag = _build_dag()
        self._highlighter = DAGHighlighter(self._dag)

    def test_highlight_by_tag_returns_matching_tasks(self):
        result = self._highlighter.highlight_by_tag("io")
        ids = [e.task_id for e in result.entries]
        assert "extract" in ids
        assert "load" in ids
        assert "transform" not in ids

    def test_highlight_by_tag_no_match_empty_result(self):
        result = self._highlighter.highlight_by_tag("nonexistent")
        assert result.has_highlights() is False

    def test_highlight_by_prefix(self):
        result = self._highlighter.highlight_by_prefix("ext")
        assert result.count() == 1
        assert result.entries[0].task_id == "extract"

    def test_highlight_by_ids_valid(self):
        result = self._highlighter.highlight_by_ids(["transform", "load"], reason="review")
        ids = [e.task_id for e in result.entries]
        assert "transform" in ids
        assert "load" in ids

    def test_highlight_by_ids_unknown_raises(self):
        with pytest.raises(HighlightError, match="ghost"):
            self._highlighter.highlight_by_ids(["ghost"])

    def test_highlight_by_tag_sets_color(self):
        result = self._highlighter.highlight_by_tag("io", color="green")
        for entry in result.entries:
            assert entry.color == "green"
