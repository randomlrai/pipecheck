"""Tests for pipecheck reporter module."""
import pytest
from pipecheck.reporter import ValidationReport, ReportEntry, Severity


class TestValidationReport:
    def setup_method(self):
        self.report = ValidationReport(dag_name="test_dag")

    def test_report_creation(self):
        assert self.report.dag_name == "test_dag"
        assert self.report.entries == []
        assert not self.report.has_errors

    def test_add_error(self):
        self.report.add_error("E001", "Cyclic dependency detected", task_id="task_a")
        assert len(self.report.errors) == 1
        assert self.report.has_errors
        entry = self.report.errors[0]
        assert entry.severity == Severity.ERROR
        assert entry.code == "E001"
        assert entry.task_id == "task_a"

    def test_add_warning(self):
        self.report.add_warning("W001", "High timeout value", task_id="task_b")
        assert len(self.report.warnings) == 1
        assert not self.report.has_errors

    def test_add_info(self):
        self.report.add_info("I001", "DAG has 5 tasks")
        assert len(self.report.entries) == 1
        assert self.report.entries[0].severity == Severity.INFO

    def test_errors_and_warnings_separate(self):
        self.report.add_error("E001", "Some error")
        self.report.add_warning("W001", "Some warning")
        self.report.add_info("I001", "Some info")
        assert len(self.report.errors) == 1
        assert len(self.report.warnings) == 1
        assert len(self.report.entries) == 3

    def test_summary_passed(self):
        self.report.add_warning("W001", "Minor issue")
        summary = self.report.summary()
        assert "PASSED" in summary
        assert "test_dag" in summary
        assert "1 warning" in summary

    def test_summary_failed(self):
        self.report.add_error("E001", "Critical issue")
        summary = self.report.summary()
        assert "FAILED" in summary
        assert "1 error" in summary

    def test_to_text_empty(self):
        text = self.report.to_text()
        assert "PASSED" in text
        assert "0 error" in text

    def test_to_text_with_entries(self):
        self.report.add_error("E001", "Bad cycle", task_id="task_x")
        text = self.report.to_text()
        assert "E001" in text
        assert "task_x" in text
        assert "Bad cycle" in text

    def test_to_dict_structure(self):
        self.report.add_error("E001", "An error", task_id="t1")
        result = self.report.to_dict()
        assert result["dag_name"] == "test_dag"
        assert result["status"] == "failed"
        assert result["error_count"] == 1
        assert result["warning_count"] == 0
        assert len(result["entries"]) == 1
        assert result["entries"][0]["task_id"] == "t1"

    def test_report_entry_str_with_task(self):
        entry = ReportEntry(Severity.WARNING, "W002", "Orphan task", task_id="orphan")
        text = str(entry)
        assert "WARNING" in text
        assert "[orphan]" in text
        assert "W002" in text

    def test_report_entry_str_no_task(self):
        entry = ReportEntry(Severity.INFO, "I001", "General info")
        text = str(entry)
        assert "[" not in text
        assert "INFO" in text
