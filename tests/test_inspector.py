"""Tests for pipecheck.inspector."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.inspector import DAGInspector, InspectionReport, TaskInspection


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task("ingest", metadata={"description": "Load data", "timeout": 30, "tags": ["io"]}))
    dag.add_task(Task("transform", dependencies=["ingest"], metadata={"tags": ["compute"]}))
    dag.add_task(Task("export", dependencies=["transform"], metadata={"timeout": 60}))
    return dag


class TestTaskInspection:
    def test_str_no_optional_fields(self):
        ti = TaskInspection(
            task_id="foo",
            description=None,
            timeout=None,
            tags=[],
            upstream=[],
            downstream=["bar"],
            depth=0,
        )
        text = str(ti)
        assert "Task: foo" in text
        assert "(none)" in text
        assert "bar" in text

    def test_str_with_all_fields(self):
        ti = TaskInspection(
            task_id="ingest",
            description="Load data",
            timeout=30,
            tags=["io"],
            upstream=[],
            downstream=["transform"],
            depth=0,
        )
        text = str(ti)
        assert "Load data" in text
        assert "30s" in text
        assert "io" in text
        assert "depth" in text.lower()


class TestDAGInspector:
    def setup_method(self):
        self.dag = _build_dag()
        self.inspector = DAGInspector(self.dag)
        self.report = self.inspector.inspect()

    def test_report_is_inspection_report(self):
        assert isinstance(self.report, InspectionReport)

    def test_report_length_matches_task_count(self):
        assert len(self.report) == 3

    def test_dag_name_preserved(self):
        assert self.report.dag_name == "test_dag"

    def test_root_task_has_depth_zero(self):
        assert self.report.get("ingest").depth == 0

    def test_middle_task_depth(self):
        assert self.report.get("transform").depth == 1

    def test_leaf_task_depth(self):
        assert self.report.get("export").depth == 2

    def test_upstream_populated(self):
        assert self.report.get("transform").upstream == ["ingest"]

    def test_downstream_populated(self):
        assert self.report.get("ingest").downstream == ["transform"]

    def test_root_has_no_upstream(self):
        assert self.report.get("ingest").upstream == []

    def test_leaf_has_no_downstream(self):
        assert self.report.get("export").downstream == []

    def test_metadata_description_extracted(self):
        assert self.report.get("ingest").description == "Load data"

    def test_metadata_timeout_extracted(self):
        assert self.report.get("ingest").timeout == 30

    def test_metadata_tags_extracted(self):
        assert "io" in self.report.get("ingest").tags

    def test_missing_task_returns_none(self):
        assert self.report.get("nonexistent") is None
