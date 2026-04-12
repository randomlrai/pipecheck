"""Integration tests: DAGScoper working end-to-end with a realistic DAG."""
from pipecheck.dag import DAG, Task
from pipecheck.scoper import DAGScoper


def _pipeline() -> DAG:
    dag = DAG(name="etl_pipeline")
    tasks = [
        Task("read_s3", metadata={"scope": "io", "timeout": 30}),
        Task("read_db", metadata={"scope": "io", "timeout": 60}),
        Task("clean", metadata={"scope": "transform"}),
        Task("enrich", metadata={"scope": "transform"}),
        Task("validate", metadata={"scope": "quality"}),
        Task("write_dw", metadata={"scope": "io", "timeout": 120}),
        Task("notify", metadata={}),
    ]
    for t in tasks:
        dag.add_task(t)
    dag.add_dependency("read_s3", "clean")
    dag.add_dependency("read_db", "clean")
    dag.add_dependency("clean", "enrich")
    dag.add_dependency("enrich", "validate")
    dag.add_dependency("validate", "write_dw")
    dag.add_dependency("write_dw", "notify")
    return dag


class TestScopeIntegration:
    def setup_method(self):
        self.dag = _pipeline()
        self.scoper = DAGScoper(self.dag)

    def test_io_scope_finds_three_tasks(self):
        result = self.scoper.scope("io")
        assert result.count == 3
        ids = {t.task_id for t in result.tasks}
        assert ids == {"read_s3", "read_db", "write_dw"}

    def test_transform_scope_finds_two_tasks(self):
        result = self.scoper.scope("transform")
        assert result.count == 2

    def test_quality_scope_finds_one_task(self):
        result = self.scoper.scope("quality")
        assert result.count == 1
        assert result.tasks[0].task_id == "validate"

    def test_task_without_scope_not_in_any_scope(self):
        result = self.scoper.scope("io")
        ids = {t.task_id for t in result.tasks}
        assert "notify" not in ids

    def test_all_scopes_returns_three_labels(self):
        scopes = self.scoper.all_scopes()
        assert scopes == ["io", "quality", "transform"]

    def test_result_str_lists_task_ids(self):
        result = self.scoper.scope("io")
        s = str(result)
        assert "read_s3" in s or "read_db" in s or "write_dw" in s
