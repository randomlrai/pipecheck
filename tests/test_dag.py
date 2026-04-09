"""Tests for DAG data structures and validation."""

import pytest
from pipecheck.dag import DAG, Task


class TestTask:
    """Test Task class."""

    def test_task_creation(self):
        task = Task(name="extract", dependencies=["validate"])
        assert task.name == "extract"
        assert task.dependencies == ["validate"]
        assert task.metadata == {}

    def test_task_with_metadata(self):
        task = Task(name="transform", metadata={"timeout": 300})
        assert task.metadata["timeout"] == 300


class TestDAG:
    """Test DAG class."""

    def test_dag_creation(self):
        dag = DAG(name="test_pipeline")
        assert dag.name == "test_pipeline"
        assert len(dag.tasks) == 0

    def test_add_task(self):
        dag = DAG()
        task = Task(name="extract")
        dag.add_task(task)
        assert "extract" in dag.tasks
        assert dag.get_task("extract") == task

    def test_duplicate_task_raises_error(self):
        dag = DAG()
        task1 = Task(name="extract")
        task2 = Task(name="extract")
        dag.add_task(task1)
        with pytest.raises(ValueError, match="already exists"):
            dag.add_task(task2)

    def test_validate_missing_dependency(self):
        dag = DAG()
        dag.add_task(Task(name="extract"))
        dag.add_task(Task(name="transform", dependencies=["nonexistent"]))
        errors = dag.validate()
        assert len(errors) == 1
        assert "nonexistent" in errors[0]

    def test_validate_cycle_detection(self):
        dag = DAG()
        dag.add_task(Task(name="task_a", dependencies=["task_b"]))
        dag.add_task(Task(name="task_b", dependencies=["task_a"]))
        errors = dag.validate()
        assert any("cycle" in error.lower() for error in errors)

    def test_validate_valid_dag(self):
        dag = DAG()
        dag.add_task(Task(name="extract"))
        dag.add_task(Task(name="transform", dependencies=["extract"]))
        dag.add_task(Task(name="load", dependencies=["transform"]))
        errors = dag.validate()
        assert len(errors) == 0

    def test_execution_order(self):
        dag = DAG()
        dag.add_task(Task(name="extract"))
        dag.add_task(Task(name="transform", dependencies=["extract"]))
        dag.add_task(Task(name="load", dependencies=["transform"]))
        order = dag.get_execution_order()
        assert order.index("extract") < order.index("transform")
        assert order.index("transform") < order.index("load")

    def test_execution_order_with_cycle(self):
        dag = DAG()
        dag.add_task(Task(name="task_a", dependencies=["task_b"]))
        dag.add_task(Task(name="task_b", dependencies=["task_a"]))
        order = dag.get_execution_order()
        assert order == []
