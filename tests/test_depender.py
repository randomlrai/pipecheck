"""Tests for pipecheck.depender."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.depender import DAGDepender, DependError, DependResult


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    for tid in ("extract", "transform", "validate", "load"):
        dag.add_task(Task(task_id=tid))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "validate")
    dag.add_edge("validate", "load")
    return dag


class TestDependResult:
    def test_str_no_deps(self):
        r = DependResult(task_id="solo", dag_name="d")
        out = str(r)
        assert "solo" in out
        assert "none" in out

    def test_str_with_values(self):
        r = DependResult(
            task_id="transform",
            dag_name="pipeline",
            upstream=["extract"],
            downstream=["validate"],
            all_ancestors=["extract"],
            all_descendants=["validate", "load"],
        )
        out = str(r)
        assert "extract" in out
        assert "validate" in out
        assert "load" in out


class TestDAGDepender:
    def setup_method(self):
        self.dag = _build_dag()
        self.depender = DAGDepender(self.dag)

    def test_analyze_unknown_task_raises(self):
        with pytest.raises(DependError, match="not found"):
            self.depender.analyze("missing")

    def test_root_has_no_upstream(self):
        result = self.depender.analyze("extract")
        assert result.upstream == []
        assert result.all_ancestors == []

    def test_root_has_downstream(self):
        result = self.depender.analyze("extract")
        assert "transform" in result.downstream

    def test_leaf_has_no_downstream(self):
        result = self.depender.analyze("load")
        assert result.downstream == []
        assert result.all_descendants == []

    def test_middle_task_upstream_and_downstream(self):
        result = self.depender.analyze("transform")
        assert "extract" in result.upstream
        assert "validate" in result.downstream

    def test_all_ancestors_transitive(self):
        result = self.depender.analyze("load")
        assert set(result.all_ancestors) == {"extract", "transform", "validate"}

    def test_all_descendants_transitive(self):
        result = self.depender.analyze("extract")
        assert set(result.all_descendants) == {"transform", "validate", "load"}

    def test_result_dag_name_stored(self):
        result = self.depender.analyze("extract")
        assert result.dag_name == "pipeline"

    def test_result_task_id_stored(self):
        result = self.depender.analyze("transform")
        assert result.task_id == "transform"
