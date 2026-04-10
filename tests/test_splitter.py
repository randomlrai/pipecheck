"""Tests for pipecheck.splitter."""

from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.splitter import DAGSplitter, SplitError, SplitResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    for tid in ("a", "b", "c", "d", "e"):
        dag.add_task(Task(task_id=tid, name=tid.upper()))
    # Component 1: a -> b -> c
    dag.add_dependency("a", "b")
    dag.add_dependency("b", "c")
    # Component 2: d -> e
    dag.add_dependency("d", "e")
    return dag


class TestSplitResult:
    def test_str_format(self):
        r = SplitResult(source_name="my_dag", parts=[])
        assert "my_dag" in str(r)
        assert "0" in str(r)

    def test_count_property(self):
        dag = DAG(name="x")
        r = SplitResult(source_name="x", parts=[dag, dag])
        assert r.count == 2


class TestDAGSplitter_method(self):
        self.splitter = DAGSplitter()

    def test_empty_dag_raises(self):
        dag = DAG(name="empty")
        with pytest.raises(SplitError):
            self.splitter.split(dag)

    def test_two_components_detected(self):
        dag = _build_dag()
        result = self.splitter.split(dag)
        assert result.count == 2

    def test_source_name_preserved(self):
        dag = _build_dag(name="pipeline")
        result = self.splitter.split(dag)
        assert result.source_name == "pipeline"

    def test_part_names_include_index(self):
        dag = _build_dag()
        result = self.splitter.split(dag)
        names = {p.name for p in result.parts}
        assert "test_part1" in names or "test_part2" in names

    def test_tasks_distributed_correctly(self):
        dag = _build_dag()
        result = self.splitter.split(dag)
        total_tasks = sum(len(p.tasks) for p in result.parts)
        assert total_tasks == 5

    def test_no_task_appears_in_multiple_parts(self):
        dag = _build_dag()
        result = self.splitter.split(dag)
        all_ids: list[str] = []
        for part in result.parts:
            all_ids.extend(part.tasks.keys())
        assert len(all_ids) == len(set(all_ids))

    def test_single_component_dag(self):
        dag = DAG(name="chain")
        for tid in ("x", "y", "z"):
            dag.add_task(Task(task_id=tid, name=tid))
        dag.add_dependency("x", "y")
        dag.add_dependency("y", "z")
        result = self.splitter.split(dag)
        assert result.count == 1
        assert len(result.parts[0].tasks) == 3

    def test_isolated_nodes_each_form_own_component(self):
        dag = DAG(name="isolated")
        for tid in ("p", "q", "r"):
            dag.add_task(Task(task_id=tid, name=tid))
        result = self.splitter.split(dag)
        assert result.count == 3

    def test_dependencies_preserved_within_part(self):
        dag = _build_dag()
        result = self.splitter.split(dag)
        # Find the part containing 'a'
        part_a = next(p for p in result.parts if "a" in p.tasks)
        assert "b" in part_a.dependencies.get("a", [])
