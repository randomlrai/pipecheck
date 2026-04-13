"""Tests for pipecheck.patcher."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.patcher import DAGPatcher, PatchEntry, PatchError, PatchResult


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="ingest", description="Load data", timeout=30))
    dag.add_task(Task(task_id="transform", description="Clean data", timeout=60))
    dag.add_task(Task(task_id="export", description="Write output", timeout=15))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "export")
    return dag


class TestPatchEntry:
    def test_str_format(self):
        entry = PatchEntry(task_id="t1", field="timeout", old_value=30, new_value=60)
        assert "t1.timeout" in str(entry)
        assert "30" in str(entry)
        assert "60" in str(entry)


class TestPatchResult:
    def test_has_patches_false_when_empty(self):
        r = PatchResult(dag_name="my_dag")
        assert r.has_patches is False

    def test_has_patches_true_when_populated(self):
        r = PatchResult(dag_name="my_dag")
        r.patches.append(PatchEntry("t", "timeout", 10, 20))
        assert r.has_patches is True

    def test_count_property(self):
        r = PatchResult(dag_name="my_dag")
        r.patches.append(PatchEntry("t", "timeout", 10, 20))
        r.patches.append(PatchEntry("t", "description", "old", "new"))
        assert r.count == 2

    def test_str_no_patches(self):
        r = PatchResult(dag_name="my_dag")
        assert "no changes" in str(r)

    def test_str_with_patches(self):
        r = PatchResult(dag_name="my_dag")
        r.patches.append(PatchEntry("t1", "timeout", 10, 20))
        out = str(r)
        assert "my_dag" in out
        assert "1 change" in out
        assert "t1.timeout" in out


class TestDAGPatcher:
    def setup_method(self):
        self.dag = _build_dag()
        self.patcher = DAGPatcher()

    def test_patch_single_field(self):
        result = self.patcher.patch(self.dag, {"ingest": {"timeout": 99}})
        assert result.has_patches
        assert result.count == 1
        assert result.patches[0].new_value == 99
        task = next(t for t in self.dag.tasks if t.task_id == "ingest")
        assert task.timeout == 99

    def test_patch_multiple_tasks(self):
        result = self.patcher.patch(
            self.dag,
            {"ingest": {"timeout": 5}, "export": {"timeout": 7}},
        )
        assert result.count == 2

    def test_patch_description_field(self):
        result = self.patcher.patch(
            self.dag, {"transform": {"description": "Updated desc"}}
        )
        assert result.patches[0].field == "description"
        task = next(t for t in self.dag.tasks if t.task_id == "transform")
        assert task.description == "Updated desc"

    def test_missing_task_raises_patch_error(self):
        with pytest.raises(PatchError, match="not found"):
            self.patcher.patch(self.dag, {"ghost": {"timeout": 1}})

    def test_missing_field_raises_patch_error(self):
        with pytest.raises(PatchError, match="no field"):
            self.patcher.patch(self.dag, {"ingest": {"nonexistent_field": 1}})

    def test_empty_spec_returns_no_patches(self):
        result = self.patcher.patch(self.dag, {})
        assert result.has_patches is False
        assert result.count == 0
