"""Tests for pipecheck.linker."""

from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.linker import DAGLinker, LinkEntry, LinkResult


def _build_dag(name: str, task_ids: list[str]) -> DAG:
    dag = DAG(name=name)
    for tid in task_ids:
        dag.add_task(Task(task_id=tid, name=tid))
    return dag


class TestLinkEntry:
    def test_str_format(self):
        entry = LinkEntry(
            source_dag="pipeline_a",
            source_task="extract",
            target_dag="pipeline_b",
            target_task="load",
        )
        assert str(entry) == "pipeline_a.extract -> pipeline_b.load"


class TestLinkResult:
    def test_has_unresolved_false_when_empty(self):
        result = LinkResult()
        assert result.has_unresolved is False

    def test_has_unresolved_true(self):
        entry = LinkEntry("a", "t1", "b", "t2")
        result = LinkResult(unresolved=[entry])
        assert result.has_unresolved is True

    def test_str_shows_counts(self):
        e1 = LinkEntry("a", "t1", "b", "t2")
        result = LinkResult(resolved=[e1])
        assert "Resolved: 1" in str(result)
        assert "Unresolved: 0" in str(result)

    def test_str_shows_missing_entries(self):
        entry = LinkEntry("a", "t1", "b", "missing")
        result = LinkResult(unresolved=[entry])
        assert "MISSING" in str(result)
        assert "a.t1 -> b.missing" in str(result)


class TestDAGLinker:
    def setup_method(self):
        self.dag_a = _build_dag("dag_a", ["ingest", "transform"])
        self.dag_b = _build_dag("dag_b", ["load", "notify"])
        self.linker = DAGLinker()
        self.linker.register(self.dag_a)
        self.linker.register(self.dag_b)

    def test_resolved_valid_link(self):
        entry = LinkEntry("dag_a", "transform", "dag_b", "load")
        result = self.linker.link([entry])
        assert len(result.resolved) == 1
        assert len(result.unresolved) == 0

    def test_unresolved_unknown_dag(self):
        entry = LinkEntry("dag_a", "ingest", "dag_c", "load")
        result = self.linker.link([entry])
        assert result.has_unresolved
        assert len(result.unresolved) == 1

    def test_unresolved_unknown_task(self):
        entry = LinkEntry("dag_a", "ingest", "dag_b", "nonexistent")
        result = self.linker.link([entry])
        assert result.has_unresolved

    def test_mixed_resolved_and_unresolved(self):
        entries = [
            LinkEntry("dag_a", "ingest", "dag_b", "load"),
            LinkEntry("dag_a", "ingest", "dag_b", "ghost"),
        ]
        result = self.linker.link(entries)
        assert len(result.resolved) == 1
        assert len(result.unresolved) == 1

    def test_register_overwrites_existing(self):
        new_dag = _build_dag("dag_a", ["only_task"])
        self.linker.register(new_dag)
        entry = LinkEntry("x", "y", "dag_a", "ingest")
        result = self.linker.link([entry])
        assert result.has_unresolved  # 'ingest' no longer exists

    def test_empty_links_returns_empty_result(self):
        result = self.linker.link([])
        assert not result.has_unresolved
        assert len(result.resolved) == 0
