"""Integration tests for DAGFilterer with realistic DAG shapes."""
from __future__ import annotations
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.filterer import DAGFilterer


def _pipeline() -> DAG:
    dag = DAG(name="etl_pipeline")
    tasks = [
        Task("extract_db", timeout=45,
             metadata={"tags": ["io", "extract"]}),
        Task("extract_api", timeout=20,
             metadata={"tags": ["io", "extract"]}),
        Task("clean_data", timeout=90,
             metadata={"tags": ["compute"]}),
        Task("aggregate", timeout=300,
             metadata={"tags": ["compute"]}),
        Task("export_csv", timeout=15,
             metadata={"tags": ["io", "export"]}),
        Task("notify_slack",
             metadata={"tags": ["notify"]}),
    ]
    for t in tasks:
        dag.add_task(t)
    dag.add_edge("extract_db", "clean_data")
    dag.add_edge("extract_api", "clean_data")
    dag.add_edge("clean_data", "aggregate")
    dag.add_edge("aggregate", "export_csv")
    dag.add_edge("export_csv", "notify_slack")
    return dag


class TestFiltererIntegration:
    def setup_method(self):
        self.dag = _pipeline()
        self.f = DAGFilterer(self.dag)

    def test_io_tag_returns_three_tasks(self):
        r = self.f.by_tag("io")
        assert r.count == 3

    def test_extract_prefix_returns_two(self):
        r = self.f.by_prefix("extract")
        assert r.count == 2

    def test_max_timeout_50_excludes_long_tasks(self):
        r = self.f.by_max_timeout(50)
        ids = [t.task_id for t in r.matched]
        assert "aggregate" not in ids
        assert "clean_data" not in ids
        assert "extract_db" in ids

    def test_no_timeout_returns_notify_only(self):
        r = self.f.by_no_timeout()
        assert r.count == 1
        assert r.matched[0].task_id == "notify_slack"

    def test_excluded_count_is_complement(self):
        r = self.f.by_tag("compute")
        assert len(r.excluded) == len(self.dag.tasks) - r.count

    def test_str_output_contains_summary_counts(self):
        r = self.f.by_tag("io")
        s = str(r)
        assert "3" in s
        assert "etl_pipeline" in s
