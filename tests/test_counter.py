"""Tests for pipecheck.counter."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.counter import CountError, CountResult, DAGCounter


def _build_dag(name: str = "pipe") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task(task_id="extract", command="run extract"))
    dag.add_task(Task(task_id="transform", command="run transform", dependencies=["extract"]))
    dag.add_task(Task(task_id="load", command="run load", dependencies=["transform"]))
    return dag


class TestCountResult:
    def test_str_contains_dag_name(self):
        r = CountResult(dag_name="mypipe", task_count=3)
        assert "mypipe" in str(r)

    def test_str_contains_counts(self):
        r = CountResult(dag_name="p", task_count=5, edge_count=4, root_count=1,
                        leaf_count=1, isolated_count=0, tag_count=2)
        s = str(r)
        assert "tasks     : 5" in s
        assert "edges     : 4" in s
        assert "roots     : 1" in s
        assert "leaves    : 1" in s
        assert "isolated  : 0" in s
        assert "unique tags: 2" in s


class TestDAGCounter:
    def setup_method(self):
        self.counter = DAGCounter()

    def test_none_dag_raises(self):
        with pytest.raises(CountError):
            self.counter.count(None)

    def test_chain_task_count(self):
        dag = _build_dag()
        result = self.counter.count(dag)
        assert result.task_count == 3

    def test_chain_edge_count(self):
        dag = _build_dag()
        result = self.counter.count(dag)
        assert result.edge_count == 2

    def test_chain_root_count(self):
        dag = _build_dag()
        result = self.counter.count(dag)
        assert result.root_count == 1

    def test_chain_leaf_count(self):
        dag = _build_dag()
        result = self.counter.count(dag)
        assert result.leaf_count == 1

    def test_chain_no_isolated(self):
        dag = _build_dag()
        result = self.counter.count(dag)
        assert result.isolated_count == 0

    def test_isolated_task_counted(self):
        dag = _build_dag()
        dag.add_task(Task(task_id="orphan", command="do nothing"))
        result = self.counter.count(dag)
        assert result.isolated_count == 1

    def test_empty_dag_all_zeros(self):
        dag = DAG(name="empty")
        result = self.counter.count(dag)
        assert result.task_count == 0
        assert result.edge_count == 0
        assert result.root_count == 0
        assert result.leaf_count == 0
        assert result.isolated_count == 0

    def test_dag_name_stored(self):
        dag = _build_dag(name="my_pipeline")
        result = self.counter.count(dag)
        assert result.dag_name == "my_pipeline"

    def test_tag_count(self):
        dag = DAG(name="tagged")
        dag.add_task(Task(task_id="a", command="a", tags=["io", "raw"]))
        dag.add_task(Task(task_id="b", command="b", tags=["io", "processed"]))
        result = self.counter.count(dag)
        assert result.tag_count == 3  # io, raw, processed
