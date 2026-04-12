"""Tests for pipecheck.anchorer."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.anchorer import AnchorEntry, AnchorError, AnchorResult, DAGAnchorer


def _build_dag() -> DAG:
    dag = DAG(name="pipe")
    for tid in ("extract", "transform", "load"):
        dag.add_task(Task(id=tid))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestAnchorEntry:
    def test_str_no_optional_fields(self):
        e = AnchorEntry(task_id="extract")
        assert str(e) == "[ANCHOR] extract"

    def test_str_with_reason(self):
        e = AnchorEntry(task_id="load", reason="stable endpoint")
        assert "stable endpoint" in str(e)
        assert "[ANCHOR] load" in str(e)

    def test_str_with_version(self):
        e = AnchorEntry(task_id="transform", pinned_version="1.2.3")
        assert "v1.2.3" in str(e)

    def test_str_with_all_fields(self):
        e = AnchorEntry(task_id="extract", reason="baseline", pinned_version="2.0")
        s = str(e)
        assert "v2.0" in s
        assert "baseline" in s


class TestAnchorResult:
    def test_has_anchors_false_when_empty(self):
        r = AnchorResult(dag_name="pipe")
        assert not r.has_anchors()

    def test_has_anchors_true_when_populated(self):
        r = AnchorResult(dag_name="pipe", anchors=[AnchorEntry("extract")])
        assert r.has_anchors()

    def test_count_property(self):
        r = AnchorResult(
            dag_name="pipe",
            anchors=[AnchorEntry("extract"), AnchorEntry("load")],
        )
        assert r.count() == 2

    def test_task_ids(self):
        r = AnchorResult(
            dag_name="pipe",
            anchors=[AnchorEntry("extract"), AnchorEntry("load")],
        )
        assert set(r.task_ids()) == {"extract", "load"}

    def test_str_no_anchors(self):
        r = AnchorResult(dag_name="pipe")
        assert "no anchors" in str(r)

    def test_str_with_anchors(self):
        r = AnchorResult(dag_name="pipe", anchors=[AnchorEntry("extract")])
        s = str(r)
        assert "1 anchor" in s
        assert "[ANCHOR] extract" in s


class TestDAGAnchorer:
    def test_anchor_valid_task(self):
        dag = _build_dag()
        anchorer = DAGAnchorer(dag)
        entry = anchorer.anchor("extract")
        assert entry.task_id == "extract"

    def test_anchor_unknown_task_raises(self):
        dag = _build_dag()
        anchorer = DAGAnchorer(dag)
        with pytest.raises(AnchorError, match="not found"):
            anchorer.anchor("nonexistent")

    def test_is_anchored_true_after_anchor(self):
        dag = _build_dag()
        anchorer = DAGAnchorer(dag)
        anchorer.anchor("transform")
        assert anchorer.is_anchored("transform")

    def test_is_anchored_false_before_anchor(self):
        dag = _build_dag()
        anchorer = DAGAnchorer(dag)
        assert not anchorer.is_anchored("extract")

    def test_release_removes_anchor(self):
        dag = _build_dag()
        anchorer = DAGAnchorer(dag)
        anchorer.anchor("load")
        anchorer.release("load")
        assert not anchorer.is_anchored("load")

    def test_release_unknown_raises(self):
        dag = _build_dag()
        anchorer = DAGAnchorer(dag)
        with pytest.raises(AnchorError, match="not anchored"):
            anchorer.release("extract")

    def test_build_result_reflects_current_state(self):
        dag = _build_dag()
        anchorer = DAGAnchorer(dag)
        anchorer.anchor("extract", reason="source", pinned_version="3.0")
        anchorer.anchor("load")
        result = anchorer.build_result()
        assert result.count() == 2
        assert "extract" in result.task_ids()
        assert "load" in result.task_ids()
