"""Tests for pipecheck.renamer."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.renamer import DAGRenamer, RenameEntry, RenameError, RenameResult


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="extract", name="Extract", depends_on=[]))
    dag.add_task(Task(task_id="transform", name="Transform", depends_on=["extract"]))
    dag.add_task(Task(task_id="load", name="Load", depends_on=["transform"]))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestRenameEntry:
    def test_str_format(self):
        entry = RenameEntry(old_id="old", new_id="new")
        assert str(entry) == "old -> new"


class TestRenameResult:
    def test_has_renames_true(self):
        dag = DAG(name="x")
        result = RenameResult(dag=dag, renames=[RenameEntry("a", "b")])
        assert result.has_renames is True

    def test_has_renames_false_when_empty(self):
        dag = DAG(name="x")
        result = RenameResult(dag=dag)
        assert result.has_renames is False

    def test_str_contains_dag_name(self):
        dag = DAG(name="mypipe")
        result = RenameResult(dag=dag, renames=[RenameEntry("a", "b")])
        assert "mypipe" in str(result)

    def test_str_shows_renamed_and_skipped(self):
        dag = DAG(name="pipe")
        result = RenameResult(
            dag=dag,
            renames=[RenameEntry("extract", "ingest")],
            skipped=["load"],
        )
        text = str(result)
        assert "renamed" in text
        assert "skipped" in text


class TestDAGRenamer:
    def setup_method(self):
        self.renamer = DAGRenamer()
        self.dag = _build_dag()

    def test_rename_changes_task_id(self):
        result = self.renamer.rename(self.dag, {"extract": "ingest"})
        assert "ingest" in result.dag.tasks
        assert "extract" not in result.dag.tasks

    def test_rename_preserves_other_tasks(self):
        result = self.renamer.rename(self.dag, {"extract": "ingest"})
        assert "transform" in result.dag.tasks
        assert "load" in result.dag.tasks

    def test_rename_updates_dependencies(self):
        result = self.renamer.rename(self.dag, {"extract": "ingest"})
        transform = result.dag.tasks["transform"]
        assert "ingest" in transform.depends_on
        assert "extract" not in transform.depends_on

    def test_rename_result_entries(self):
        result = self.renamer.rename(self.dag, {"extract": "ingest"})
        assert len(result.renames) == 1
        assert result.renames[0].old_id == "extract"
        assert result.renames[0].new_id == "ingest"

    def test_rename_skipped_lists_unchanged_tasks(self):
        result = self.renamer.rename(self.dag, {"extract": "ingest"})
        assert "transform" in result.skipped
        assert "load" in result.skipped

    def test_unknown_task_raises(self):
        with pytest.raises(RenameError, match="not found"):
            self.renamer.rename(self.dag, {"missing": "new_id"})

    def test_duplicate_target_raises(self):
        with pytest.raises(RenameError, match="duplicate"):
            self.renamer.rename(self.dag, {"extract": "same", "transform": "same"})

    def test_target_conflicts_with_existing_raises(self):
        with pytest.raises(RenameError, match="already exists"):
            self.renamer.rename(self.dag, {"extract": "load"})

    def test_dag_name_preserved(self):
        result = self.renamer.rename(self.dag, {"extract": "ingest"})
        assert result.dag.name == "pipeline"
