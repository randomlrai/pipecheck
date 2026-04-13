"""Tests for pipecheck.indexer."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.indexer import DAGIndexer, IndexEntry, IndexError, IndexResult


def _build_dag() -> DAG:
    dag = DAG(name="test_pipeline")
    dag.add_task(Task("ingest", metadata={"owner": "alice", "env": "prod"}))
    dag.add_task(Task("transform", metadata={"owner": "bob", "env": "prod"}))
    dag.add_task(Task("export", metadata={"owner": "alice", "team": "data"}))
    dag.add_task(Task("notify", metadata={"team": "ops"}))
    return dag


class TestIndexEntry:
    def test_str_format(self):
        entry = IndexEntry(task_id="ingest", attribute="owner", value="alice")
        assert str(entry) == "ingest: owner=alice"


class TestIndexResult:
    def test_has_entries_false_when_empty(self):
        result = IndexResult(dag_name="my_dag")
        assert result.has_entries() is False

    def test_has_entries_true_when_populated(self):
        result = IndexResult(dag_name="my_dag")
        result.entries.append(IndexEntry("t1", "owner", "alice"))
        assert result.has_entries() is True

    def test_count_property(self):
        result = IndexResult(dag_name="my_dag")
        result.entries.append(IndexEntry("t1", "owner", "alice"))
        result.entries.append(IndexEntry("t2", "owner", "bob"))
        assert result.count() == 2

    def test_tasks_for_value(self):
        result = IndexResult(dag_name="my_dag")
        result.entries.append(IndexEntry("t1", "owner", "alice"))
        result.entries.append(IndexEntry("t2", "owner", "bob"))
        result.entries.append(IndexEntry("t3", "owner", "alice"))
        assert sorted(result.tasks_for_value("alice")) == ["t1", "t3"]

    def test_str_empty(self):
        result = IndexResult(dag_name="my_dag")
        assert "no entries" in str(result)

    def test_str_non_empty(self):
        result = IndexResult(dag_name="my_dag")
        result.entries.append(IndexEntry("t1", "owner", "alice"))
        text = str(result)
        assert "my_dag" in text
        assert "t1" in text
        assert "owner=alice" in text


class TestDAGIndexer:
    def setup_method(self):
        self.dag = _build_dag()
        self.indexer = DAGIndexer()

    def test_index_by_owner_returns_correct_count(self):
        result = self.indexer.index(self.dag, "owner")
        assert result.count() == 3

    def test_index_by_team_returns_correct_count(self):
        result = self.indexer.index(self.dag, "team")
        assert result.count() == 2

    def test_index_by_env_returns_prod_tasks(self):
        result = self.indexer.index(self.dag, "env")
        task_ids = [e.task_id for e in result.entries]
        assert "ingest" in task_ids
        assert "transform" in task_ids
        assert "export" not in task_ids

    def test_entries_sorted_by_value_then_task_id(self):
        result = self.indexer.index(self.dag, "owner")
        values = [(e.value, e.task_id) for e in result.entries]
        assert values == sorted(values)

    def test_unsupported_attribute_raises(self):
        with pytest.raises(IndexError, match="Unsupported attribute"):
            self.indexer.index(self.dag, "nonexistent")

    def test_dag_name_stored(self):
        result = self.indexer.index(self.dag, "owner")
        assert result.dag_name == "test_pipeline"

    def test_task_without_attribute_excluded(self):
        result = self.indexer.index(self.dag, "version")
        assert result.count() == 0
