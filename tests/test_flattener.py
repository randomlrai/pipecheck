"""Tests for pipecheck.flattener."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.flattener import DAGFlattener, FlattenError, FlatTask, FlattenResult


def _build_dag(name: str = "test_dag") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task(task_id="ingest", command="load"))
    dag.add_task(Task(task_id="transform", command="transform", dependencies=["ingest"]))
    dag.add_task(Task(task_id="export", command="export", dependencies=["transform"]))
    return dag


class TestFlatTask:
    def test_str_no_upstream(self):
        ft = FlatTask(task_id="start", depth=0)
        assert "depth=0" in str(ft)
        assert "start" in str(ft)
        assert "none" in str(ft)

    def test_str_with_upstream(self):
        ft = FlatTask(task_id="end", depth=2, all_upstream=["a", "b"])
        result = str(ft)
        assert "depth=2" in result
        assert "a" in result
        assert "b" in result


class TestFlattenResult:
    def test_str_contains_dag_name(self):
        r = FlattenResult(dag_name="my_dag")
        assert "my_dag" in str(r)

    def test_str_shows_task_count(self):
        r = FlattenResult(dag_name="my_dag", tasks=[FlatTask("a", 0)])
        assert "1 task" in str(r)

    def test_task_ids_property(self):
        r = FlattenResult(
            dag_name="d",
            tasks=[FlatTask("a", 0), FlatTask("b", 1)],
        )
        assert r.task_ids == ["a", "b"]


class TestDAGFlattener:
    def test_flatten_returns_result(self):
        dag = _build_dag()
        result = DAGFlattener().flatten(dag)
        assert isinstance(result, FlattenResult)
        assert result.dag_name == "test_dag"

    def test_flatten_task_count(self):
        dag = _build_dag()
        result = DAGFlattener().flatten(dag)
        assert len(result.tasks) == 3

    def test_depths_are_correct(self):
        dag = _build_dag()
        result = DAGFlattener().flatten(dag)
        depth_map = {ft.task_id: ft.depth for ft in result.tasks}
        assert depth_map["ingest"] == 0
        assert depth_map["transform"] == 1
        assert depth_map["export"] == 2

    def test_all_upstream_transitive(self):
        dag = _build_dag()
        result = DAGFlattener().flatten(dag)
        export_task = next(ft for ft in result.tasks if ft.task_id == "export")
        assert "ingest" in export_task.all_upstream
        assert "transform" in export_task.all_upstream

    def test_root_task_has_no_upstream(self):
        dag = _build_dag()
        result = DAGFlattener().flatten(dag)
        ingest = next(ft for ft in result.tasks if ft.task_id == "ingest")
        assert ingest.all_upstream == []

    def test_empty_dag_raises(self):
        dag = DAG(name="empty")
        with pytest.raises(FlattenError, match="no tasks"):
            DAGFlattener().flatten(dag)

    def test_metadata_preserved(self):
        dag = DAG(name="meta_dag")
        dag.add_task(Task(task_id="t1", command="run", metadata={"owner": "alice"}))
        result = DAGFlattener().flatten(dag)
        assert result.tasks[0].metadata["owner"] == "alice"

    def test_parallel_tasks_same_depth(self):
        dag = DAG(name="parallel")
        dag.add_task(Task(task_id="a", command="a"))
        dag.add_task(Task(task_id="b", command="b"))
        dag.add_task(Task(task_id="c", command="c", dependencies=["a", "b"]))
        result = DAGFlattener().flatten(dag)
        depth_map = {ft.task_id: ft.depth for ft in result.tasks}
        assert depth_map["a"] == 0
        assert depth_map["b"] == 0
        assert depth_map["c"] == 1
