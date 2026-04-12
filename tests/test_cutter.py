"""Tests for pipecheck.cutter."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.cutter import CutError, CutResult, DAGCutter


def _build_dag() -> DAG:
    """Build a simple linear + branch DAG for testing.

    extract -> transform -> load
                         -> report
    """
    dag = DAG(name="pipeline")
    for tid in ("extract", "transform", "load", "report"):
        dag.add_task(Task(task_id=tid))
    dag.add_dependency("transform", "extract")
    dag.add_dependency("load", "transform")
    dag.add_dependency("report", "transform")
    return dag


class TestCutResult:
    def test_str_format(self):
        result = CutResult(
            dag_name="pipeline",
            cut_task_id="transform",
            upstream=[Task(task_id="extract")],
            downstream=[Task(task_id="load"), Task(task_id="report")],
        )
        text = str(result)
        assert "transform" in text
        assert "pipeline" in text
        assert "1" in text  # upstream count
        assert "2" in text  # downstream count

    def test_has_upstream_false_when_empty(self):
        result = CutResult(dag_name="d", cut_task_id="t")
        assert result.has_upstream is False

    def test_has_upstream_true_when_populated(self):
        result = CutResult(
            dag_name="d",
            cut_task_id="t",
            upstream=[Task(task_id="a")],
        )
        assert result.has_upstream is True

    def test_has_downstream_false_when_empty(self):
        result = CutResult(dag_name="d", cut_task_id="t")
        assert result.has_downstream is False

    def test_has_downstream_true_when_populated(self):
        result = CutResult(
            dag_name="d",
            cut_task_id="t",
            downstream=[Task(task_id="b")],
        )
        assert result.has_downstream is True


class TestDAGCutter:
    def setup_method(self):
        self.dag = _build_dag()
        self.cutter = DAGCutter()

    def test_cut_at_root_has_no_upstream(self):
        result = self.cutter.cut(self.dag, "extract")
        assert result.has_upstream is False

    def test_cut_at_root_has_downstream(self):
        result = self.cutter.cut(self.dag, "extract")
        downstream_ids = {t.task_id for t in result.downstream}
        assert downstream_ids == {"transform", "load", "report"}

    def test_cut_at_middle_has_upstream(self):
        result = self.cutter.cut(self.dag, "transform")
        upstream_ids = {t.task_id for t in result.upstream}
        assert upstream_ids == {"extract"}

    def test_cut_at_middle_has_downstream(self):
        result = self.cutter.cut(self.dag, "transform")
        downstream_ids = {t.task_id for t in result.downstream}
        assert downstream_ids == {"load", "report"}

    def test_cut_at_leaf_has_no_downstream(self):
        result = self.cutter.cut(self.dag, "load")
        assert result.has_downstream is False

    def test_cut_at_leaf_has_upstream(self):
        result = self.cutter.cut(self.dag, "load")
        upstream_ids = {t.task_id for t in result.upstream}
        assert "extract" in upstream_ids
        assert "transform" in upstream_ids

    def test_unknown_task_raises_cut_error(self):
        with pytest.raises(CutError, match="missing"):
            self.cutter.cut(self.dag, "missing")

    def test_result_stores_dag_name(self):
        result = self.cutter.cut(self.dag, "transform")
        assert result.dag_name == "pipeline"

    def test_result_stores_cut_task_id(self):
        result = self.cutter.cut(self.dag, "transform")
        assert result.cut_task_id == "transform"
