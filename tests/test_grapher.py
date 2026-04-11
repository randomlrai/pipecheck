"""Tests for pipecheck.grapher."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.grapher import DAGGrapher, GraphMetrics


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="ingest", command="cmd_a"))
    dag.add_task(Task(task_id="transform", command="cmd_b", dependencies=["ingest"]))
    dag.add_task(Task(task_id="load", command="cmd_c", dependencies=["transform"]))
    dag.add_task(Task(task_id="report", command="cmd_d", dependencies=["transform"]))
    return dag


class TestGraphMetrics:
    def test_str_contains_dag_name(self):
        m = GraphMetrics("my_dag", 3, 2, 2, ["a"], ["c"])
        assert "my_dag" in str(m)

    def test_str_contains_counts(self):
        m = GraphMetrics("d", 4, 3, 2, ["x"], ["y"])
        s = str(m)
        assert "4 nodes" in s
        assert "3 edges" in s
        assert "depth=2" in s

    def test_str_shows_roots_and_leaves(self):
        m = GraphMetrics("d", 2, 1, 1, ["root_a"], ["leaf_b"])
        s = str(m)
        assert "root_a" in s
        assert "leaf_b" in s


class TestDAGGrapher:
    def test_grapher_creation(self):
        dag = _build_dag()
        grapher = DAGGrapher(dag)
        assert grapher is not None

    def test_node_count(self):
        dag = _build_dag()
        metrics = DAGGrapher(dag).analyze()
        assert metrics.node_count == 4

    def test_edge_count(self):
        dag = _build_dag()
        metrics = DAGGrapher(dag).analyze()
        assert metrics.edge_count == 3

    def test_root_tasks(self):
        dag = _build_dag()
        metrics = DAGGrapher(dag).analyze()
        assert metrics.root_tasks == ["ingest"]

    def test_leaf_tasks(self):
        dag = _build_dag()
        metrics = DAGGrapher(dag).analyze()
        assert sorted(metrics.leaf_tasks) == ["load", "report"]

    def test_max_depth(self):
        dag = _build_dag()
        metrics = DAGGrapher(dag).analyze()
        assert metrics.max_depth == 2

    def test_reachable_from_root(self):
        dag = _build_dag()
        metrics = DAGGrapher(dag).analyze()
        assert metrics.reachable_from["ingest"] == {"transform", "load", "report"}

    def test_reachable_from_leaf_is_empty(self):
        dag = _build_dag()
        metrics = DAGGrapher(dag).analyze()
        assert metrics.reachable_from["load"] == set()

    def test_empty_dag_returns_zero_metrics(self):
        dag = DAG(name="empty")
        metrics = DAGGrapher(dag).analyze()
        assert metrics.node_count == 0
        assert metrics.edge_count == 0
        assert metrics.max_depth == 0
        assert metrics.root_tasks == []
        assert metrics.leaf_tasks == []

    def test_single_node_dag(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only", command="run"))
        metrics = DAGGrapher(dag).analyze()
        assert metrics.node_count == 1
        assert metrics.edge_count == 0
        assert metrics.root_tasks == ["only"]
        assert metrics.leaf_tasks == ["only"]
        assert metrics.max_depth == 0
