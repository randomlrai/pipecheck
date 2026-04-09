"""Tests for DAG validator."""

import pytest
from pipecheck.dag import DAG, Task
from pipecheck.validator import DAGValidator, ValidationError, CyclicDependencyError, OrphanTaskError


class TestDAGValidator:
    """Test suite for DAGValidator."""

    def test_valid_dag(self):
        """Test validation of a valid DAG."""
        dag = DAG(name="valid_dag")
        task1 = Task(task_id="task1")
        task2 = Task(task_id="task2", dependencies=["task1"])
        task3 = Task(task_id="task3", dependencies=["task2"])
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        validator = DAGValidator(dag)
        is_valid, errors, warnings = validator.validate()
        
        assert is_valid is True
        assert len(errors) == 0

    def test_cyclic_dependency_detection(self):
        """Test detection of cyclic dependencies."""
        dag = DAG(name="cyclic_dag")
        task1 = Task(task_id="task1", dependencies=["task3"])
        task2 = Task(task_id="task2", dependencies=["task1"])
        task3 = Task(task_id="task3", dependencies=["task2"])
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        validator = DAGValidator(dag)
        is_valid, errors, warnings = validator.validate()
        
        assert is_valid is False
        assert len(errors) > 0
        assert any(isinstance(e, CyclicDependencyError) for e in errors)

    def test_orphan_task_warning(self):
        """Test warning for orphaned tasks."""
        dag = DAG(name="orphan_dag")
        task1 = Task(task_id="task1")
        task2 = Task(task_id="task2", dependencies=["task1"])
        task3 = Task(task_id="orphan")
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        validator = DAGValidator(dag)
        is_valid, errors, warnings = validator.validate()
        
        assert is_valid is True
        assert len(warnings) > 0
        assert any("orphan" in w.lower() for w in warnings)

    def test_high_timeout_warning(self):
        """Test warning for tasks with high timeout."""
        dag = DAG(name="timeout_dag")
        task1 = Task(task_id="task1", metadata={"timeout": 100000})
        
        dag.add_task(task1)
        
        validator = DAGValidator(dag)
        is_valid, errors, warnings = validator.validate()
        
        assert is_valid is True
        assert len(warnings) > 0
        assert any("timeout" in w.lower() for w in warnings)

    def test_high_retry_warning(self):
        """Test warning for tasks with high retry count."""
        dag = DAG(name="retry_dag")
        task1 = Task(task_id="task1", metadata={"retries": 15})
        
        dag.add_task(task1)
        
        validator = DAGValidator(dag)
        is_valid, errors, warnings = validator.validate()
        
        assert is_valid is True
        assert len(warnings) > 0
        assert any("retry" in w.lower() for w in warnings)

    def test_empty_dag(self):
        """Test validation of empty DAG."""
        dag = DAG(name="empty_dag")
        validator = DAGValidator(dag)
        is_valid, errors, warnings = validator.validate()
        
        assert is_valid is True
        assert len(errors) == 0
