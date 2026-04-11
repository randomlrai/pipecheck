"""Tests for pipecheck.sorter."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.sorter import DAGSorter, SortError, SortResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task(task_id="ingest", dependencies=[]))
    dag.add_task(Task(task_id="transform", dependencies=["ingest"]))
    dag.add_task(Task(task_id="validate", dependencies=["ingest"]))
    dag.add_task(Task(task_id="load", dependencies=["transform", "validate"]))
    return dag


class TestSortResult:
    def test_count_property(self):
        result = SortResult(dag_name="d", order=["a", "b", "c"])
        assert result.count == 3

    def test_count_empty(self):
        result = SortResult(dag_name="d")
        assert result.count == 0

    def test_dag_name_stored(self):
        result = SortResult(dag_name="my_dag", order=["x"])
        assert result.dag_name == "my_dag"


class TestDAGSorter:
    def test_sorter_creation(self):
        dag = _build_dag()
        sorter = DAGSorter(dag)
        assert sorter is not None

    def test_linear_dag_order(self):
        dag = DAG(name="linear")
        dag.add_task(Task(task_id="a", dependencies=[]))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        dag.add_task(Task(task_id="c", dependencies=["b"]))
        result = DAGSorter(dag).sort()
        assert result.order == ["a", "b", "c"]

    def test_diamond_dag_respects_deps(self):
        dag = _build_dag()
        result = DAGSorter(dag).sort()
        assert result.order.index("ingest") < result.order.index("transform")
        assert result.order.index("ingest") < result.order.index("validate")
        assert result.order.index("transform") < result.order.index("load")
        assert result.order.index("validate") < result.order.index("load")

    def test_result_contains_all_tasks(self):
        dag = _build_dag()
        result = DAGSorter(dag).sort()
        assert set(result.order) == {"ingest", "transform", "validate", "load"}
        assert result.count == 4

    def test_dag_name_propagated(self):
        dag = _build_dag(name="pipeline_x")
        result = DAGSorter(dag).sort()
        assert result.dag_name == "pipeline_x"

    def test_single_task_dag(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only", dependencies=[]))
        result = DAGSorter(dag).sort()
        assert result.order == ["only"]

    def test_cyclic_dag_raises_sort_error(self):
        dag = DAG(name="cyclic")
        dag.add_task(Task(task_id="a", dependencies=["b"]))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        with pytest.raises(SortError, match="cycle"):
            DAGSorter(dag).sort()

    def test_unknown_dependency_raises_sort_error(self):
        dag = DAG(name="broken")
        dag.add_task(Task(task_id="a", dependencies=["ghost"]))
        with pytest.raises(SortError, match="unknown task"):
            DAGSorter(dag).sort()

    def test_empty_dag(self):
        dag = DAG(name="empty")
        result = DAGSorter(dag).sort()
        assert result.order == []
        assert result.count == 0
