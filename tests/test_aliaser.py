"""Tests for pipecheck.aliaser."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.aliaser import AliasEntry, AliasResult, AliasError, DAGAliaser


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="ingest", command="python ingest.py"))
    dag.add_task(Task(task_id="transform", command="python transform.py"))
    dag.add_task(Task(task_id="load", command="python load.py"))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestAliasEntry:
    def test_str_format(self):
        entry = AliasEntry(task_id="ingest", alias="Data Ingestion")
        assert str(entry) == "ingest -> Data Ingestion"


class TestAliasResult:
    def test_has_aliases_false_when_empty(self):
        result = AliasResult(dag_name="test")
        assert result.has_aliases is False

    def test_has_aliases_true_when_entries_present(self):
        result = AliasResult(
            dag_name="test",
            entries=[AliasEntry(task_id="a", alias="Alpha")],
        )
        assert result.has_aliases is True

    def test_has_unresolved_false_when_empty(self):
        result = AliasResult(dag_name="test")
        assert result.has_unresolved is False

    def test_has_unresolved_true_when_set(self):
        result = AliasResult(dag_name="test", unresolved=["missing_task"])
        assert result.has_unresolved is True

    def test_str_contains_dag_name(self):
        result = AliasResult(dag_name="my_pipeline")
        assert "my_pipeline" in str(result)

    def test_str_shows_none_when_no_entries(self):
        result = AliasResult(dag_name="test")
        assert "(none)" in str(result)

    def test_str_shows_entries(self):
        result = AliasResult(
            dag_name="test",
            entries=[AliasEntry(task_id="ingest", alias="Ingestion Step")],
        )
        assert "ingest -> Ingestion Step" in str(result)

    def test_str_shows_unresolved(self):
        result = AliasResult(dag_name="test", unresolved=["ghost_task"])
        assert "ghost_task" in str(result)
        assert "Unresolved" in str(result)


class TestDAGAliaser:
    def setup_method(self):
        self.dag = _build_dag()
        self.aliaser = DAGAliaser()

    def test_alias_known_tasks(self):
        result = self.aliaser.alias(self.dag, {"ingest": "Data Ingestion", "load": "Load Step"})
        assert result.has_aliases
        assert len(result.entries) == 2
        assert not result.has_unresolved

    def test_alias_entries_sorted_by_task_id(self):
        result = self.aliaser.alias(self.dag, {"transform": "T", "ingest": "I"})
        ids = [e.task_id for e in result.entries]
        assert ids == sorted(ids)

    def test_unresolved_tracked(self):
        result = self.aliaser.alias(self.dag, {"nonexistent": "Ghost"})
        assert "nonexistent" in result.unresolved
        assert not result.has_aliases

    def test_empty_alias_raises(self):
        with pytest.raises(AliasError):
            self.aliaser.alias(self.dag, {"ingest": "  "})

    def test_lookup_returns_alias(self):
        result = self.aliaser.alias(self.dag, {"ingest": "Ingestion"})
        assert self.aliaser.lookup(result, "ingest") == "Ingestion"

    def test_lookup_returns_none_for_unaliased(self):
        result = self.aliaser.alias(self.dag, {"ingest": "Ingestion"})
        assert self.aliaser.lookup(result, "transform") is None

    def test_dag_name_stored(self):
        result = self.aliaser.alias(self.dag, {})
        assert result.dag_name == "pipeline"
