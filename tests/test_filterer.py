"""Tests for pipecheck.filterer."""
from __future__ import annotations
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.filterer import DAGFilterer, FilterResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task("ingest", timeout=30,
                      metadata={"tags": ["io", "source"]}))
    dag.add_task(Task("transform", timeout=120,
                      metadata={"tags": ["compute"]}))
    dag.add_task(Task("load", timeout=60,
                      metadata={"tags": ["io", "sink"]}))
    dag.add_task(Task("notify", metadata={"tags": []}))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestFilterResult:
    def test_count_property(self):
        r = FilterResult(dag_name="d", matched=[], excluded=[])
        assert r.count == 0

    def test_has_matches_false_when_empty(self):
        r = FilterResult(dag_name="d", matched=[], excluded=[])
        assert not r.has_matches

    def test_has_matches_true_when_populated(self):
        t = Task("a")
        r = FilterResult(dag_name="d", matched=[t], excluded=[])
        assert r.has_matches

    def test_str_contains_dag_name(self):
        r = FilterResult(dag_name="my_dag", matched=[], excluded=[])
        assert "my_dag" in str(r)

    def test_str_shows_matched_task_ids(self):
        t = Task("alpha")
        r = FilterResult(dag_name="d", matched=[t], excluded=[])
        assert "alpha" in str(r)


class TestDAGFilterer:
    def setup_method(self):
        self.dag = _build_dag()
        self.filterer = DAGFilterer(self.dag)

    def test_by_tag_returns_matching(self):
        result = self.filterer.by_tag("io")
        ids = [t.task_id for t in result.matched]
        assert "ingest" in ids
        assert "load" in ids
        assert "transform" not in ids

    def test_by_tag_no_match_empty_result(self):
        result = self.filterer.by_tag("nonexistent")
        assert result.count == 0
        assert not result.has_matches

    def test_by_prefix_matches_correctly(self):
        result = self.filterer.by_prefix("in")
        ids = [t.task_id for t in result.matched]
        assert "ingest" in ids
        assert "transform" not in ids

    def test_by_max_timeout_filters_correctly(self):
        result = self.filterer.by_max_timeout(60)
        ids = [t.task_id for t in result.matched]
        assert "ingest" in ids   # 30
        assert "load" in ids     # 60
        assert "transform" not in ids  # 120

    def test_by_no_timeout_returns_tasks_without_timeout(self):
        result = self.filterer.by_no_timeout()
        ids = [t.task_id for t in result.matched]
        assert "notify" in ids
        assert "ingest" not in ids

    def test_excluded_plus_matched_equals_total(self):
        result = self.filterer.by_tag("io")
        total = len(self.dag.tasks)
        assert len(result.matched) + len(result.excluded) == total
