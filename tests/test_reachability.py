"""Tests for pipecheck.reachability."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.reachability import (
    DAGReachabilityAnalyzer,
    ReachabilityError,
    ReachabilityResult,
)


def _build_dag() -> DAG:
    dag = DAG(name="pipe")
    for tid in ("extract", "transform", "load", "notify"):
        dag.add_task(Task(id=tid))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "notify")
    return dag


class TestReachabilityResult:
    def test_can_reach_direct(self):
        result = ReachabilityResult(
            dag_name="test",
            reachable={"a": frozenset({"b", "c"}), "b": frozenset({"c"}), "c": frozenset()},
        )
        assert result.can_reach("a", "b") is True

    def test_can_reach_transitive(self):
        result = ReachabilityResult(
            dag_name="test",
            reachable={"a": frozenset({"b", "c"}), "b": frozenset({"c"}), "c": frozenset()},
        )
        assert result.can_reach("a", "c") is True

    def test_cannot_reach_unreachable(self):
        result = ReachabilityResult(
            dag_name="test",
            reachable={"a": frozenset({"b"}), "b": frozenset(), "c": frozenset()},
        )
        assert result.can_reach("b", "a") is False

    def test_unreachable_from(self):
        result = ReachabilityResult(
            dag_name="test",
            reachable={"a": frozenset({"b"}), "b": frozenset(), "c": frozenset()},
        )
        assert "c" in result.unreachable_from("a")

    def test_str_contains_dag_name(self):
        result = ReachabilityResult(dag_name="mypipe", reachable={})
        assert "mypipe" in str(result)

    def test_str_shows_none_when_no_targets(self):
        result = ReachabilityResult(
            dag_name="x", reachable={"a": frozenset()}
        )
        assert "(none)" in str(result)

    def test_can_reach_unknown_source_raises(self):
        """can_reach should raise KeyError when the source task is not in the result."""
        result = ReachabilityResult(
            dag_name="test",
            reachable={"a": frozenset({"b"}), "b": frozenset()},
        )
        with pytest.raises(KeyError):
            result.can_reach("unknown", "b")


class TestDAGReachabilityAnalyzer:
    def test_analyze_chain(self):
        dag = _build_dag()
        result = DAGReachabilityAnalyzer(dag).analyze()
        assert result.can_reach("extract", "notify")
        assert result.can_reach("extract", "load")
        assert not result.can_reach("notify", "extract")

    def test_analyze_returns_correct_type(self):
        result = DAGReachabilityAnalyzer(_build_dag()).analyze()
        assert isinstance(result, ReachabilityResult)

    def test_analyze_dag_name_preserved(self):
        result = DAGReachabilityAnalyzer(_build_dag()).analyze()
        assert result.dag_name == "pipe"

    def test_analyze_empty_dag_raises(self):
        dag = DAG(name="empty")
        with pytest.raises(ReachabilityError):
            DAGReachabilityAnalyzer(dag).analyze()

    def test_isolated_task_reaches_nothing(self):
        dag = DAG(name="iso")
        dag.add_task(Task(id="alone"))
        dag.add_task(Task(id="other"))
        result = DAGReachabilityAnalyzer(dag).analyze()
        assert result.reachable["alone"] == frozenset()

    def test_diamond_reachability(
