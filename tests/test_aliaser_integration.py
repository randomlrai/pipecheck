"""Integration tests for DAGAliaser with realistic DAG structures."""

from pipecheck.dag import DAG, Task
from pipecheck.aliaser import DAGAliaser


def _complex_dag() -> DAG:
    dag = DAG(name="etl_pipeline")
    tasks = [
        Task(task_id="extract_raw", command="python extract.py"),
        Task(task_id="validate_schema", command="python validate.py"),
        Task(task_id="transform_data", command="python transform.py"),
        Task(task_id="enrich_data", command="python enrich.py"),
        Task(task_id="load_warehouse", command="python load.py"),
    ]
    for t in tasks:
        dag.add_task(t)
    dag.add_edge("extract_raw", "validate_schema")
    dag.add_edge("validate_schema", "transform_data")
    dag.add_edge("transform_data", "enrich_data")
    dag.add_edge("enrich_data", "load_warehouse")
    return dag


class TestAliasIntegration:
    def setup_method(self):
        self.dag = _complex_dag()
        self.aliaser = DAGAliaser()

    def test_full_pipeline_aliased(self):
        mapping = {
            "extract_raw": "Extract Raw Data",
            "validate_schema": "Validate Schema",
            "transform_data": "Transform Data",
            "enrich_data": "Enrich Data",
            "load_warehouse": "Load to Warehouse",
        }
        result = self.aliaser.alias(self.dag, mapping)
        assert len(result.entries) == 5
        assert not result.has_unresolved

    def test_partial_alias_leaves_rest_unaliased(self):
        result = self.aliaser.alias(self.dag, {"extract_raw": "Extract"})
        assert len(result.entries) == 1
        assert self.aliaser.lookup(result, "validate_schema") is None

    def test_mixed_valid_and_invalid(self):
        mapping = {
            "extract_raw": "Extract",
            "nonexistent_task": "Ghost",
        }
        result = self.aliaser.alias(self.dag, mapping)
        assert len(result.entries) == 1
        assert "nonexistent_task" in result.unresolved

    def test_result_str_round_trip(self):
        mapping = {"extract_raw": "Extract", "load_warehouse": "Load"}
        result = self.aliaser.alias(self.dag, mapping)
        output = str(result)
        assert "etl_pipeline" in output
        assert "extract_raw -> Extract" in output
        assert "load_warehouse -> Load" in output

    def test_empty_mapping_produces_empty_result(self):
        result = self.aliaser.alias(self.dag, {})
        assert not result.has_aliases
        assert not result.has_unresolved
