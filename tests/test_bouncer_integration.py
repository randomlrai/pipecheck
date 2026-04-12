"""Integration tests for DAGBouncer with realistic pipeline DAGs."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.bouncer import DAGBouncer


def _etl_dag() -> DAG:
    dag = DAG(name="etl_pipeline")
    tasks = [
        Task("ingest_orders", metadata={"tags": ["io", "source"]}),
        Task("ingest_customers", metadata={"tags": ["io", "source"]}),
        Task("join_data", metadata={"tags": ["compute"]}),
        Task("validate", metadata={"tags": ["compute", "quality"]}),
        Task("write_output", metadata={"tags": ["io", "sink"]}),
        Task("alert", metadata={"tags": ["ops"]}),
    ]
    for t in tasks:
        dag.add_task(t)
    dag.add_edge("ingest_orders", "join_data")
    dag.add_edge("ingest_customers", "join_data")
    dag.add_edge("join_data", "validate")
    dag.add_edge("validate", "write_output")
    dag.add_edge("write_output", "alert")
    return dag


class TestBouncerIntegration:
    def setup_method(self):
        self.dag = _etl_dag()
        self.bouncer = DAGBouncer(self.dag)

    def test_deny_io_removes_three_tasks(self):
        result = self.bouncer.evaluate(deny_tags=["io"])
        denied_ids = {e.task_id for e in result.denied}
        assert denied_ids == {"ingest_orders", "ingest_customers", "write_output"}

    def test_allow_compute_only_two_tasks_allowed(self):
        result = self.bouncer.evaluate(allow_tags=["compute"])
        allowed_ids = {e.task_id for e in result.allowed}
        assert allowed_ids == {"join_data", "validate"}

    def test_deny_prefix_ingest_blocks_two(self):
        result = self.bouncer.evaluate(deny_prefixes=["ingest"])
        denied_ids = {e.task_id for e in result.denied}
        assert "ingest_orders" in denied_ids
        assert "ingest_customers" in denied_ids
        assert len(denied_ids) == 2

    def test_combined_allow_and_deny(self):
        # allow io but deny sink
        result = self.bouncer.evaluate(
            allow_tags=["io"],
            deny_tags=["sink"],
        )
        allowed_ids = {e.task_id for e in result.allowed}
        denied_ids = {e.task_id for e in result.denied}
        # write_output has both io and sink — deny wins
        assert "write_output" in denied_ids
        assert "ingest_orders" in allowed_ids
        assert "ingest_customers" in allowed_ids

    def test_no_rules_all_six_allowed(self):
        result = self.bouncer.evaluate()
        assert len(result.allowed) == 6
        assert not result.has_denials
