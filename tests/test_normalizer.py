"""Tests for pipecheck.normalizer."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.normalizer import DAGNormalizer, NormalizeEntry, NormalizeResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task(task_id="extract", timeout=30))
    dag.add_task(Task(task_id="transform", timeout=60))
    dag.add_task(Task(task_id="load", timeout=20))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestNormalizeEntry:
    def test_str_format(self):
        entry = NormalizeEntry(task_id="My Task", original="My Task", normalized="my_task")
        assert "My Task" in str(entry)
        assert "my_task" in str(entry)
        assert "->" in str(entry)


class TestNormalizeResult:
    def test_has_changes_false_when_empty(self):
        result = NormalizeResult(dag_name="pipe")
        assert result.has_changes is False

    def test_has_changes_true_when_entries_present(self):
        result = NormalizeResult(
            dag_name="pipe",
            entries=[NormalizeEntry("A B", "A B", "a_b")],
        )
        assert result.has_changes is True

    def test_count_property(self):
        result = NormalizeResult(
            dag_name="pipe",
            entries=[
                NormalizeEntry("A", "A", "a"),
                NormalizeEntry("B C", "B C", "b_c"),
            ],
        )
        assert result.count == 2

    def test_str_no_changes(self):
        result = NormalizeResult(dag_name="pipe")
        assert "No normalization" in str(result)

    def test_str_with_changes(self):
        result = NormalizeResult(
            dag_name="pipe",
            entries=[NormalizeEntry("My Task", "My Task", "my_task")],
        )
        text = str(result)
        assert "pipe" in text
        assert "1 task" in text
        assert "my_task" in text


class TestDAGNormalizer:
    def test_already_normalized_dag_has_no_changes(self):
        dag = _build_dag()
        normalizer = DAGNormalizer()
        result = normalizer.normalize(dag)
        assert result.has_changes is False

    def test_normalizer_lowercases_ids(self):
        dag = DAG(name="upper")
        dag.add_task(Task(task_id="Extract"))
        dag.add_task(Task(task_id="Load"))
        dag.add_edge("Extract", "Load")
        result = DAGNormalizer().normalize(dag)
        assert result.has_changes is True
        assert result.count == 2
        assert "extract" in result.dag.tasks
        assert "load" in result.dag.tasks

    def test_normalizer_replaces_spaces_with_underscores(self):
        dag = DAG(name="spaced")
        dag.add_task(Task(task_id="my task"))
        result = DAGNormalizer().normalize(dag)
        assert "my_task" in result.dag.tasks

    def test_normalizer_replaces_hyphens_with_underscores(self):
        dag = DAG(name="hyphenated")
        dag.add_task(Task(task_id="my-task"))
        result = DAGNormalizer().normalize(dag)
        assert "my_task" in result.dag.tasks

    def test_edges_are_remapped_after_normalization(self):
        dag = DAG(name="edged")
        dag.add_task(Task(task_id="Step One"))
        dag.add_task(Task(task_id="Step Two"))
        dag.add_edge("Step One", "Step Two")
        result = DAGNormalizer().normalize(dag)
        assert ("step_one", "step_two") in result.dag.edges

    def test_result_dag_name_preserved(self):
        dag = _build_dag(name="my_pipeline")
        result = DAGNormalizer().normalize(dag)
        assert result.dag.name == "my_pipeline"
