"""Tests for pipecheck.namer."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.namer import DAGNamer, NameEntry, NameError, NameResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="ingest", metadata={"display_name": "Ingest Data"}))
    dag.add_task(Task(task_id="transform", metadata={"display_name": "Transform"}))
    dag.add_task(Task(task_id="load"))
    return dag


# ---------------------------------------------------------------------------
# NameEntry
# ---------------------------------------------------------------------------

class TestNameEntry:
    def test_str_format(self):
        entry = NameEntry(task_id="t1", old_name="Old", new_name="New")
        assert str(entry) == "t1: 'Old' -> 'New'"


# ---------------------------------------------------------------------------
# NameResult
# ---------------------------------------------------------------------------

class TestNameResult:
    def test_has_changes_false_when_empty(self):
        r = NameResult(dag_name="dag")
        assert r.has_changes is False

    def test_has_changes_true_when_populated(self):
        r = NameResult(dag_name="dag")
        r.entries.append(NameEntry("t1", "A", "B"))
        assert r.has_changes is True

    def test_count_property(self):
        r = NameResult(dag_name="dag")
        r.entries.append(NameEntry("t1", "A", "B"))
        r.entries.append(NameEntry("t2", "C", "D"))
        assert r.count == 2

    def test_str_no_changes(self):
        r = NameResult(dag_name="my_dag")
        text = str(r)
        assert "my_dag" in text
        assert "(no changes)" in text

    def test_str_with_changes(self):
        r = NameResult(dag_name="my_dag")
        r.entries.append(NameEntry("t1", "Old Name", "New Name"))
        text = str(r)
        assert "Old Name" in text
        assert "New Name" in text


# ---------------------------------------------------------------------------
# DAGNamer
# ---------------------------------------------------------------------------

class TestDAGNamer:
    def setup_method(self):
        self.dag = _build_dag()
        self.namer = DAGNamer(self.dag)

    def test_no_mapping_returns_empty_result(self):
        result = self.namer.apply({})
        assert result.has_changes is False
        assert result.count == 0

    def test_rename_with_display_name_metadata(self):
        result = self.namer.apply({"ingest": "Load Raw Files"})
        assert result.has_changes is True
        assert result.count == 1
        assert result.entries[0].task_id == "ingest"
        assert result.entries[0].old_name == "Ingest Data"
        assert result.entries[0].new_name == "Load Raw Files"

    def test_rename_task_without_display_name_uses_task_id(self):
        result = self.namer.apply({"load": "Final Load Step"})
        assert result.count == 1
        assert result.entries[0].old_name == "load"

    def test_same_name_produces_no_entry(self):
        result = self.namer.apply({"transform": "Transform"})
        assert result.has_changes is False

    def test_unknown_task_id_raises_name_error(self):
        with pytest.raises(NameError, match="unknown_task"):
            self.namer.apply({"unknown_task": "Whatever"})

    def test_multiple_renames(self):
        result = self.namer.apply({"ingest": "Step 1", "load": "Step 3"})
        assert result.count == 2
        ids = {e.task_id for e in result.entries}
        assert ids == {"ingest", "load"}
