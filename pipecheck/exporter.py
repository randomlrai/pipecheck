"""Export DAG validation reports to various output formats."""
from __future__ import annotations

import json
import csv
import io
from pathlib import Path
from typing import Union

from pipecheck.reporter import ValidationReport, Severity


class ExportError(Exception):
    """Raised when export fails."""


class ReportExporter:
    """Exports a ValidationReport to JSON, CSV, or plain text."""

    SUPPORTED_FORMATS = ("json", "csv", "txt")

    def __init__(self, report: ValidationReport) -> None:
        self.report = report

    def export(self, fmt: str, dest: Union[str, Path, None] = None) -> str:
        """Serialize the report in *fmt* and optionally write to *dest*.

        Returns the serialized string regardless of whether *dest* is given.
        """
        fmt = fmt.lower()
        if fmt not in self.SUPPORTED_FORMATS:
            raise ExportError(
                f"Unsupported format '{fmt}'. Choose from {self.SUPPORTED_FORMATS}."
            )

        serializers = {
            "json": self._to_json,
            "csv": self._to_csv,
            "txt": self._to_txt,
        }
        content = serializers[fmt]()

        if dest is not None:
            Path(dest).write_text(content, encoding="utf-8")

        return content

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _to_json(self) -> str:
        data = {
            "dag": self.report.dag_name,
            "passed": self.report.passed,
            "entries": [
                {
                    "severity": entry.severity.value,
                    "code": entry.code,
                    "message": entry.message,
                    "task": entry.task_id,
                }
                for entry in self.report.entries
            ],
        }
        return json.dumps(data, indent=2)

    def _to_csv(self) -> str:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["severity", "code", "message", "task"])
        for entry in self.report.entries:
            writer.writerow(
                [entry.severity.value, entry.code, entry.message, entry.task_id or ""]
            )
        return buf.getvalue()

    def _to_txt(self) -> str:
        lines = [f"Report for DAG: {self.report.dag_name}"]
        lines.append("=" * 40)
        if not self.report.entries:
            lines.append("No issues found.")
        else:
            for entry in self.report.entries:
                task_part = f" [{entry.task_id}]" if entry.task_id else ""
                lines.append(
                    f"[{entry.severity.value.upper()}] {entry.code}{task_part}: {entry.message}"
                )
        lines.append("=" * 40)
        status = "PASSED" if self.report.passed else "FAILED"
        lines.append(f"Status: {status}")
        return "\n".join(lines)
