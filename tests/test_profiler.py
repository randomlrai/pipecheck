"""Tests for pipecheck.profiler."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.profiler import DAGProfiler, DAGProfile, TaskProfile


def _build_dag() -> DAG:
    """Build a simple linear + fan-out DAG.

    ingest -> transform -> load_a
                       -> load_b
    """
    dag = DAG(dag_id="test_dag")
    dag.add_task(Task("ingest", dependencies=[]))
    dag.add_task(Task("transform", dependencies=["ingest"]))
    dag.add_task(Task("load_a", dependencies=["transform"]))
    dag.add_task(Task("load_b", dependencies=["transform"]))
    return dag


class TestTaskProfile:
    def test_str_no_bottleneck(self):
        tp = TaskProfile(task_id="t1", timeout=30, depth=1, dependents=0)
        assert "t1" in str(tp)
        assert "BOTTLENECK" not in str(tp)

    def test_str_bottleneck(self):
        tp = TaskProfile(task_id="t1", timeout=None, depth=2, dependents=3, is_bottleneck=True)
        assert "BOTTLENECK" in str(tp)


class TestDAGProfile:
    def test_summary_contains_fields(self):
        profile = DAGProfile(
            total_tasks=3,
            max_depth=2,
            critical_path=["a", "b", "c"],
            bottlenecks=["b"],
        )
        summary = profile.summary()
        assert "3" in summary
        assert "2" in summary
        assert "a -> b -> c" in summary
        assert "b" in summary

    def test_summary_no_bottlenecks(self):
        profile = DAGProfile(
            total_tasks=1, max_depth=0, critical_path=["a"], bottlenecks=[]
        )
        assert "none" in profile.summary()


class TestDAGProfiler:
    def setup_method(self):
        self.dag = _build_dag()
        self.profiler = DAGProfiler(self.dag)

    def test_profiler_creation(self):
        assert self.profiler.dag is self.dag

    def test_total_tasks(self):
        result = self.profiler.profile()
        assert result.total_tasks == 4

    def test_max_depth(self):
        result = self.profiler.profile()
        assert result.max_depth == 2

    def test_critical_path_starts_at_root(self):
        result = self.profiler.profile()
        assert result.critical_path[0] == "ingest"

    def test_critical_path_ends_at_leaf(self):
        result = self.profiler.profile()
        assert result.critical_path[-1] in ("load_a", "load_b")

    def test_transform_is_bottleneck(self):
        result = self.profiler.profile()
        assert "transform" in result.bottlenecks

    def test_leaf_tasks_not_bottleneck(self):
        result = self.profiler.profile()
        assert "load_a" not in result.bottlenecks
        assert "load_b" not in result.bottlenecks

    def test_task_profiles_populated(self):
        result = self.profiler.profile()
        assert set(result.task_profiles.keys()) == {"ingest", "transform", "load_a", "load_b"}

    def test_timeout_in_profile(self):
        dag = DAG(dag_id="t")
        dag.add_task(Task("t1", dependencies=[], metadata={"timeout": 60}))
        result = DAGProfiler(dag).profile()
        assert result.task_profiles["t1"].timeout == 60

    def test_empty_dag(self):
        dag = DAG(dag_id="empty")
        result = DAGProfiler(dag).profile()
        assert result.total_tasks == 0
        assert result.max_depth == 0
        assert result.critical_path == []
        assert result.bottlenecks == []
