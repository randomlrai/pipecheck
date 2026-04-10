"""Tests for pipecheck.cloner."""
from __future__ import annotations

import pytest

from pipecheck.cloner import CloneError, CloneResult, DAGCloner
from pipecheck.dag import DAG, Task


def _build_dag(name: str = "pipeline") -> DAG:
    dag = DAG(name=name)
    t1 = Task(task_id="extract", description="Extract data", timeout=30, tags=["etl"])
    t2 = Task(task_id="transform", description="Transform data", timeout=60)
    t3 = Task(task_id="load", description="Load data", timeout=20)
    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_task(t3)
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestCloneResult:
    def test_str_without_prefix(self):
        dag = DAG(name="clone")
        result = CloneResult(
            original_name="original",
            cloned_dag=dag,
            task_count=3,
            edge_count=2,
        )
        s = str(result)
        assert "original" in s
        assert "clone" in s
        assert "3 tasks" in s
        assert "2 edges" in s
        assert "prefix" not in s

    def test_str_with_prefix(self):
        dag = DAG(name="clone")
        result = CloneResult(
            original_name="original",
            cloned_dag=dag,
            task_count=2,
            edge_count=1,
            prefix="v2_",
        )
        assert "prefix='v2_'" in str(result)


class TestDAGCloner:
    def setup_method(self):
        self.cloner = DAGCloner()
        self.dag = _build_dag()

    def test_clone_returns_result(self):
        result = self.cloner.clone(self.dag, new_name="pipeline_copy")
        assert isinstance(result, CloneResult)

    def test_cloned_dag_has_new_name(self):
        result = self.cloner.clone(self.dag, new_name="new_pipeline")
        assert result.cloned_dag.name == "new_pipeline"

    def test_task_count_matches(self):
        result = self.cloner.clone(self.dag, new_name="copy")
        assert result.task_count == 3
        assert len(result.cloned_dag.tasks) == 3

    def test_edge_count_matches(self):
        result = self.cloner.clone(self.dag, new_name="copy")
        assert result.edge_count == 2

    def test_prefix_applied_to_task_ids(self):
        result = self.cloner.clone(self.dag, new_name="copy", prefix="v2_")
        ids = set(result.cloned_dag.tasks.keys())
        assert ids == {"v2_extract", "v2_transform", "v2_load"}

    def test_dependencies_remapped_with_prefix(self):
        result = self.cloner.clone(self.dag, new_name="copy", prefix="v2_")
        load_task = result.cloned_dag.tasks["v2_load"]
        assert "v2_transform" in load_task.dependencies

    def test_tags_and_metadata_copied(self):
        result = self.cloner.clone(self.dag, new_name="copy")
        extract = result.cloned_dag.tasks["extract"]
        assert "etl" in extract.tags

    def test_original_dag_unchanged(self):
        self.cloner.clone(self.dag, new_name="copy", prefix="x_")
        assert "extract" in self.dag.tasks

    def test_empty_dag_raises_clone_error(self):
        empty = DAG(name="empty")
        with pytest.raises(CloneError):
            self.cloner.clone(empty, new_name="copy")
