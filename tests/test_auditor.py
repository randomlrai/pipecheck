"""Tests for DAGAuditor and AuditReport."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.auditor import AuditEntry, AuditReport, DAGAuditor


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    t1 = Task(task_id="ingest", command="python ingest.py", timeout=30)
    t2 = Task(task_id="transform", command="python transform.py", metadata={"owner": "alice"})
    t3 = Task(task_id="load", command="python load.py", timeout=60)
    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_task(t3)
    dag.add_dependency(dependent="transform", dependency="ingest")
    dag.add_dependency(dependent="load", dependency="transform")
    return dag


class TestAuditEntry:
    def test_str_format(self):
        entry = AuditEntry(category="structure", key="task_count", value=3)
        assert str(entry) == "[structure] task_count: 3"


class TestAuditReport:
    def test_creation(self):
        report = AuditReport(dag_name="my_dag")
        assert report.dag_name == "my_dag"
        assert report.entries == []
        assert report.generated_at  # non-empty timestamp

    def test_add_entry(self):
        report = AuditReport(dag_name="my_dag")
        report.add("structure", "task_count", 5)
        assert len(report) == 1
        assert report.entries[0].key == "task_count"

    def test_to_dict_structure(self):
        report = AuditReport(dag_name="my_dag")
        report.add("coverage", "timeout_coverage_pct", 50.0)
        d = report.to_dict()
        assert d["dag_name"] == "my_dag"
        assert "generated_at" in d
        assert d["entries"][0]["category"] == "coverage"
        assert d["entries"][0]["value"] == 50.0


class TestDAGAuditor:
    def test_audit_returns_report(self):
        dag = _build_dag()
        report = DAGAuditor().audit(dag)
        assert isinstance(report, AuditReport)
        assert report.dag_name == "test_dag"

    def test_task_count(self):
        dag = _build_dag()
        report = DAGAuditor().audit(dag)
        entry = next(e for e in report.entries if e.key == "task_count")
        assert entry.value == 3

    def test_edge_count(self):
        dag = _build_dag()
        report = DAGAuditor().audit(dag)
        entry = next(e for e in report.entries if e.key == "edge_count")
        assert entry.value == 2

    def test_root_tasks(self):
        dag = _build_dag()
        report = DAGAuditor().audit(dag)
        entry = next(e for e in report.entries if e.key == "root_tasks")
        assert entry.value == ["ingest"]

    def test_leaf_tasks(self):
        dag = _build_dag()
        report = DAGAuditor().audit(dag)
        entry = next(e for e in report.entries if e.key == "leaf_tasks")
        assert entry.value == ["load"]

    def test_timeout_coverage(self):
        dag = _build_dag()
        report = DAGAuditor().audit(dag)
        entry = next(e for e in report.entries if e.key == "timeout_coverage_pct")
        assert entry.value == pytest.approx(66.7, rel=0.01)

    def test_metadata_coverage(self):
        dag = _build_dag()
        report = DAGAuditor().audit(dag)
        entry = next(e for e in report.entries if e.key == "tasks_with_metadata")
        assert entry.value == ["transform"]

    def test_empty_dag(self):
        dag = DAG(name="empty")
        report = DAGAuditor().audit(dag)
        task_count = next(e for e in report.entries if e.key == "task_count")
        assert task_count.value == 0
