"""Integration tests for DAGPatcher applied to realistic DAGs."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.patcher import DAGPatcher, PatchError


def _etl_dag() -> DAG:
    dag = DAG(name="etl")
    tasks = [
        Task(task_id="extract", description="Pull from source", timeout=120),
        Task(task_id="validate", description="Check schema", timeout=30),
        Task(task_id="load", description="Write to warehouse", timeout=90),
    ]
    for t in tasks:
        dag.add_task(t)
    dag.add_edge("extract", "validate")
    dag.add_edge("validate", "load")
    return dag


class TestPatcherIntegration:
    def setup_method(self):
        self.dag = _etl_dag()
        self.patcher = DAGPatcher()

    def test_patch_preserves_other_tasks(self):
        self.patcher.patch(self.dag, {"extract": {"timeout": 200}})
        validate = next(t for t in self.dag.tasks if t.task_id == "validate")
        assert validate.timeout == 30

    def test_patch_multiple_fields_same_task(self):
        result = self.patcher.patch(
            self.dag,
            {"load": {"timeout": 45, "description": "Write to S3"}},
        )
        assert result.count == 2
        load = next(t for t in self.dag.tasks if t.task_id == "load")
        assert load.timeout == 45
        assert load.description == "Write to S3"

    def test_patch_records_old_values_correctly(self):
        result = self.patcher.patch(self.dag, {"extract": {"timeout": 1}})
        assert result.patches[0].old_value == 120
        assert result.patches[0].new_value == 1

    def test_sequential_patches_accumulate(self):
        r1 = self.patcher.patch(self.dag, {"extract": {"timeout": 50}})
        r2 = self.patcher.patch(self.dag, {"extract": {"timeout": 75}})
        assert r1.patches[0].new_value == 50
        assert r2.patches[0].old_value == 50
        assert r2.patches[0].new_value == 75

    def test_patch_all_tasks_in_one_call(self):
        spec = {
            "extract": {"timeout": 10},
            "validate": {"timeout": 10},
            "load": {"timeout": 10},
        }
        result = self.patcher.patch(self.dag, spec)
        assert result.count == 3
        for t in self.dag.tasks:
            assert t.timeout == 10
