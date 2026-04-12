"""Tests for pipecheck.cycler."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.cycler import CycleResult, DAGCycler


def _build_dag(name: str = "test_dag") -> DAG:
    dag = DAG(name=name)
    return dag


def _add_tasks(dag: DAG, *task_ids: str) -> None:
    for tid in task_ids:
        dag.add_task(Task(task_id=tid))


def _add_edge(dag: DAG, src: str, dst: str) -> None:
    dag.add_edge(src, dst)


class TestCycleResult:
    def test_no_cycles_has_cycles_false(self):
        result = CycleResult(dag_name="dag")
        assert not result.has_cycles

    def test_with_cycles_has_cycles_true(self):
        result = CycleResult(dag_name="dag", cycles=[["a", "b", "a"]])
        assert result.has_cycles

    def test_count_empty(self):
        result = CycleResult(dag_name="dag")
        assert result.count == 0

    def test_count_non_empty(self):
        result = CycleResult(dag_name="dag", cycles=[["a", "b", "a"], ["c", "d", "c"]])
        assert result.count == 2

    def test_str_no_cycles(self):
        result = CycleResult(dag_name="my_dag")
        text = str(result)
        assert "my_dag" in text
        assert "No cycles detected" in text

    def test_str_with_cycles(self):
        result = CycleResult(dag_name="my_dag", cycles=[["a", "b", "a"]])
        text = str(result)
        assert "1 cycle(s) found" in text
        assert "a -> b -> a" in text

    def test_str_multiple_cycles(self):
        result = CycleResult(
            dag_name="my_dag",
            cycles=[["a", "b", "a"], ["c", "d", "c"]],
        )
        text = str(result)
        assert "2 cycle(s) found" in text
        assert "[1]" in text
        assert "[2]" in text


class TestDAGCycler:
    def test_linear_dag_no_cycles(self):
        dag = _build_dag()
        _add_tasks(dag, "a", "b", "c")
        _add_edge(dag, "a", "b")
        _add_edge(dag, "b", "c")
        result = DAGCycler(dag).detect()
        assert not result.has_cycles

    def test_single_cycle_detected(self):
        dag = _build_dag()
        _add_tasks(dag, "a", "b", "c")
        _add_edge(dag, "a", "b")
        _add_edge(dag, "b", "c")
        _add_edge(dag, "c", "a")
        result = DAGCycler(dag).detect()
        assert result.has_cycles
        assert result.count == 1

    def test_self_loop_detected(self):
        dag = _build_dag()
        _add_tasks(dag, "a")
        _add_edge(dag, "a", "a")
        result = DAGCycler(dag).detect()
        assert result.has_cycles

    def test_dag_name_preserved(self):
        dag = _build_dag(name="pipeline_x")
        _add_tasks(dag, "a")
        result = DAGCycler(dag).detect()
        assert result.dag_name == "pipeline_x"

    def test_empty_dag_no_cycles(self):
        dag = _build_dag()
        result = DAGCycler(dag).detect()
        assert not result.has_cycles
        assert result.count == 0

    def test_diamond_dag_no_cycles(self):
        dag = _build_dag()
        _add_tasks(dag, "root", "left", "right", "sink")
        _add_edge(dag, "root", "left")
        _add_edge(dag, "root", "right")
        _add_edge(dag, "left", "sink")
        _add_edge(dag, "right", "sink")
        result = DAGCycler(dag).detect()
        assert not result.has_cycles
