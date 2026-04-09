"""Tests for pipecheck.exporter."""
import json
import csv
import io
import tempfile
from pathlib import Path

import pytest

from pipecheck.exporter import ReportExporter, ExportError
from pipecheck.reporter import ValidationReport, Severity


class TestReportExporter:
    def _make_report(self, passed: bool = True) -> ValidationReport:
        report = ValidationReport(dag_name="test_dag")
        if not passed:
            report.add_error("E001", "Missing dependency", task_id="task_a")
            report.add_warning("W001", "High timeout", task_id="task_b")
        return report

    # ------------------------------------------------------------------
    # JSON
    # ------------------------------------------------------------------

    def test_export_json_structure(self):
        report = self._make_report(passed=False)
        exporter = ReportExporter(report)
        raw = exporter.export("json")
        data = json.loads(raw)
        assert data["dag"] == "test_dag"
        assert isinstance(data["entries"], list)
        assert len(data["entries"]) == 2

    def test_export_json_passed_flag(self):
        report = self._make_report(passed=True)
        exporter = ReportExporter(report)
        data = json.loads(exporter.export("json"))
        assert data["passed"] is True

    def test_export_json_entry_fields(self):
        report = self._make_report(passed=False)
        exporter = ReportExporter(report)
        entry = json.loads(exporter.export("json"))["entries"][0]
        assert "severity" in entry
        assert "code" in entry
        assert "message" in entry
        assert "task" in entry

    # ------------------------------------------------------------------
    # CSV
    # ------------------------------------------------------------------

    def test_export_csv_header(self):
        report = self._make_report()
        exporter = ReportExporter(report)
        raw = exporter.export("csv")
        reader = csv.reader(io.StringIO(raw))
        header = next(reader)
        assert header == ["severity", "code", "message", "task"]

    def test_export_csv_rows(self):
        report = self._make_report(passed=False)
        exporter = ReportExporter(report)
        raw = exporter.export("csv")
        rows = list(csv.reader(io.StringIO(raw)))
        assert len(rows) == 3  # header + 2 entries

    # ------------------------------------------------------------------
    # TXT
    # ------------------------------------------------------------------

    def test_export_txt_contains_dag_name(self):
        report = self._make_report()
        exporter = ReportExporter(report)
        txt = exporter.export("txt")
        assert "test_dag" in txt

    def test_export_txt_passed_status(self):
        report = self._make_report(passed=True)
        exporter = ReportExporter(report)
        assert "PASSED" in exporter.export("txt")

    def test_export_txt_failed_status(self):
        report = self._make_report(passed=False)
        exporter = ReportExporter(report)
        assert "FAILED" in exporter.export("txt")

    # ------------------------------------------------------------------
    # File output
    # ------------------------------------------------------------------

    def test_export_writes_file(self, tmp_path):
        report = self._make_report()
        exporter = ReportExporter(report)
        dest = tmp_path / "report.json"
        exporter.export("json", dest=dest)
        assert dest.exists()
        data = json.loads(dest.read_text())
        assert data["dag"] == "test_dag"

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def test_unsupported_format_raises(self):
        report = self._make_report()
        exporter = ReportExporter(report)
        with pytest.raises(ExportError, match="Unsupported format"):
            exporter.export("xml")

    def test_case_insensitive_format(self):
        report = self._make_report()
        exporter = ReportExporter(report)
        result = exporter.export("JSON")
        assert json.loads(result)["dag"] == "test_dag"
