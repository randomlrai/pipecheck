"""Tests for pipecheck.walker."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.walker import DAGWalker, WalkError, WalkResult, WalkStep


def _build_dag(name: str = "test_dag") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task(task_id="start", command="echo start"))
    dag.add_task(Task(task_id="middle", command="echo middle", dependencies=["start"]))
    dag.add_task(Task(task_id="end", command="echo end", dependencies=["middle"]))
    return dag


class TestWalkStep:
    def test_str_no_indent(self):
        task = Task(task_id="my_task", command="run")
        step = WalkStep(depth=0, task=task)
        assert str(step) == "[0] my_task"

    def test_str_indented(self):
        task = Task(task_id="deep_task", command="run")
        step = WalkStep(depth=2, task=task)
        assert str(step) == "    [2] deep_task"


class TestWalkResult:
    def test_len(self):
        dag = _build_dag()
        result = DAGWalker(dag).walk()
        assert len(result) == 3

    def test_task_ids_in_order(self):
        dag = _build_dag()
        result = DAGWalker(dag).walk()
        ids = result.task_ids()
        assert ids.index("start") < ids.index("middle")
        assert ids.index("middle") < ids.index("end")

    def test_str_contains_dag_name(self):
        dag = _build_dag("my_pipeline")
        result = DAGWalker(dag).walk()
        assert "my_pipeline" in str(result)

    def test_str_contains_step_count(self):
        dag = _build_dag()
        result = DAGWalker(dag).walk()
        assert "3 steps" in str(result)


class TestDAGWalker:
    def test_single_task_dag(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only", command="run"))
        result = DAGWalker(dag).walk()
        assert result.task_ids() == ["only"]
        assert result.steps[0].depth == 0

    def test_depths_assigned_correctly(self):
        dag = _build_dag()
        result = DAGWalker(dag).walk()
        depth_map = {s.task.task_id: s.depth for s in result.steps}
        assert depth_map["start"] == 0
        assert depth_map["middle"] == 1
        assert depth_map["end"] == 2

    def test_diamond_dag_depths(self):
        dag = DAG(name="diamond")
        dag.add_task(Task(task_id="root", command="r"))
        dag.add_task(Task(task_id="left", command="l", dependencies=["root"]))
        dag.add_task(Task(task_id="right", command="r", dependencies=["root"]))
        dag.add_task(Task(task_id="sink", command="s", dependencies=["left", "right"]))
        result = DAGWalker(dag).walk()
        depth_map = {s.task.task_id: s.depth for s in result.steps}
        assert depth_map["root"] == 0
        assert depth_map["left"] == 1
        assert depth_map["right"] == 1
        assert depth_map["sink"] == 2

    def test_iter_steps_yields_same_as_walk(self):
        dag = _build_dag()
        walker = DAGWalker(dag)
        assert list(walker.iter_steps()) == walker.walk().steps

    def test_unknown_dependency_raises(self):
        dag = DAG(name="bad")
        dag.add_task(Task(task_id="orphan", command="x", dependencies=["ghost"]))
        with pytest.raises(WalkError, match="Unknown dependency"):
            DAGWalker(dag).walk()
