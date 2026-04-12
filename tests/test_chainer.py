"""Tests for pipecheck.chainer."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.chainer import Chain, ChainResult, DAGChainer, ChainError


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, task_id: str, **kwargs) -> Task:
    t = Task(task_id=task_id, **kwargs)
    dag.add_task(t)
    return t


# ---------------------------------------------------------------------------
# Chain
# ---------------------------------------------------------------------------

class TestChain:
    def test_str_empty(self):
        c = Chain()
        assert str(c) == "Chain(empty)"

    def test_str_single(self):
        c = Chain(task_ids=["a"])
        assert str(c) == "a"

    def test_str_multiple(self):
        c = Chain(task_ids=["a", "b", "c"])
        assert str(c) == "a -> b -> c"

    def test_len(self):
        c = Chain(task_ids=["x", "y"])
        assert len(c) == 2

    def test_len_empty(self):
        assert len(Chain()) == 0


# ---------------------------------------------------------------------------
# ChainResult
# ---------------------------------------------------------------------------

class TestChainResult:
    def test_count_empty(self):
        r = ChainResult(dag_name="d")
        assert r.count() == 0

    def test_longest_none_when_empty(self):
        r = ChainResult(dag_name="d")
        assert r.longest() is None

    def test_longest_returns_max(self):
        r = ChainResult(
            dag_name="d",
            chains=[
                Chain(task_ids=["a", "b"]),
                Chain(task_ids=["x", "y", "z"]),
            ],
        )
        assert r.longest().task_ids == ["x", "y", "z"]

    def test_str_contains_dag_name(self):
        r = ChainResult(dag_name="mypipe")
        assert "mypipe" in str(r)

    def test_str_lists_chains(self):
        r = ChainResult(
            dag_name="d",
            chains=[Chain(task_ids=["a", "b"])],
        )
        assert "a -> b" in str(r)


# ---------------------------------------------------------------------------
# DAGChainer
# ---------------------------------------------------------------------------

class TestDAGChainer:
    def test_linear_chain_detected(self):
        dag = _build_dag()
        _add(dag, "a")
        _add(dag, "b")
        _add(dag, "c")
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        result = DAGChainer(dag).extract()
        assert result.count() == 1
        assert result.chains[0].task_ids == ["a", "b", "c"]

    def test_branching_splits_chains(self):
        dag = _build_dag()
        _add(dag, "root")
        _add(dag, "left")
        _add(dag, "right")
        dag.add_edge("root", "left")
        dag.add_edge("root", "right")
        result = DAGChainer(dag).extract()
        # branching means no chain of length >= 2
        assert result.count() == 0

    def test_diamond_no_chains(self):
        dag = _build_dag()
        for tid in ["a", "b", "c", "d"]:
            _add(dag, tid)
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        result = DAGChainer(dag).extract()
        assert result.count() == 0

    def test_isolated_tasks_no_chains(self):
        dag = _build_dag()
        _add(dag, "solo")
        result = DAGChainer(dag).extract()
        assert result.count() == 0

    def test_two_independent_chains(self):
        dag = _build_dag()
        for tid in ["a", "b", "c", "d"]:
            _add(dag, tid)
        dag.add_edge("a", "b")
        dag.add_edge("c", "d")
        result = DAGChainer(dag).extract()
        assert result.count() == 2

    def test_result_dag_name(self):
        dag = _build_dag(name="pipeline_x")
        result = DAGChainer(dag).extract()
        assert result.dag_name == "pipeline_x"
