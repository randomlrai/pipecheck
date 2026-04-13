"""Tests for pipecheck.ranker."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.ranker import DAGRanker, RankEntry, RankError, RankResult


def _build_dag() -> DAG:
    """Build a simple linear DAG: ingest -> transform -> load."""
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="ingest", name="Ingest", timeout=10))
    dag.add_task(Task(task_id="transform", name="Transform", timeout=30))
    dag.add_task(Task(task_id="load", name="Load", timeout=5))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestRankEntry:
    def test_str_format(self):
        entry = RankEntry(task_id="ingest", rank=1, score=2.0, metric="degree")
        text = str(entry)
        assert "ingest" in text
        assert "#  1" in text or "1" in text
        assert "degree" in text


class TestRankResult:
    def test_count_property(self):
        result = RankResult(dag_name="p", metric="degree", entries=[])
        assert result.count == 0

    def test_top_returns_n_entries(self):
        entries = [
            RankEntry(task_id=f"t{i}", rank=i + 1, score=float(10 - i), metric="degree")
            for i in range(6)
        ]
        result = RankResult(dag_name="p", metric="degree", entries=entries)
        assert len(result.top(3)) == 3
        assert result.top(3)[0].task_id == "t0"

    def test_str_contains_dag_name(self):
        result = RankResult(dag_name="mypipe", metric="depth", entries=[])
        assert "mypipe" in str(result)

    def test_str_contains_metric(self):
        result = RankResult(dag_name="p", metric="timeout", entries=[])
        assert "timeout" in str(result)


class TestDAGRanker:
    def setup_method(self):
        self.dag = _build_dag()
        self.ranker = DAGRanker()

    def test_rank_by_degree_returns_result(self):
        result = self.ranker.rank(self.dag, metric="degree")
        assert isinstance(result, RankResult)
        assert result.count == 3

    def test_rank_by_degree_order(self):
        result = self.ranker.rank(self.dag, metric="degree")
        # transform has degree 2 (one parent + one child)
        assert result.entries[0].task_id == "transform"

    def test_rank_by_depth_order(self):
        result = self.ranker.rank(self.dag, metric="depth")
        # load is deepest (depth=2)
        assert result.entries[0].task_id == "load"

    def test_rank_by_timeout_order(self):
        result = self.ranker.rank(self.dag, metric="timeout")
        # transform has timeout=30
        assert result.entries[0].task_id == "transform"

    def test_unsupported_metric_raises(self):
        with pytest.raises(RankError, match="Unsupported metric"):
            self.ranker.rank(self.dag, metric="banana")

    def test_rank_entries_have_sequential_ranks(self):
        result = self.ranker.rank(self.dag, metric="degree")
        ranks = [e.rank for e in result.entries]
        assert ranks == list(range(1, result.count + 1))

    def test_dag_name_stored_in_result(self):
        result = self.ranker.rank(self.dag, metric="depth")
        assert result.dag_name == "pipeline"
