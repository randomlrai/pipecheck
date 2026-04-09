"""Reporting module for pipecheck validation results."""
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class Severity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ReportEntry:
    severity: Severity
    code: str
    message: str
    task_id: Optional[str] = None

    def __str__(self) -> str:
        location = f"[{self.task_id}] " if self.task_id else ""
        return f"{self.severity.value}: {location}{self.code} - {self.message}"


@dataclass
class ValidationReport:
    dag_name: str
    entries: List[ReportEntry] = field(default_factory=list)

    def add_error(self, code: str, message: str, task_id: Optional[str] = None) -> None:
        self.entries.append(ReportEntry(Severity.ERROR, code, message, task_id))

    def add_warning(self, code: str, message: str, task_id: Optional[str] = None) -> None:
        self.entries.append(ReportEntry(Severity.WARNING, code, message, task_id))

    def add_info(self, code: str, message: str, task_id: Optional[str] = None) -> None:
        self.entries.append(ReportEntry(Severity.INFO, code, message, task_id))

    @property
    def errors(self) -> List[ReportEntry]:
        return [e for e in self.entries if e.severity == Severity.ERROR]

    @property
    def warnings(self) -> List[ReportEntry]:
        return [e for e in self.entries if e.severity == Severity.WARNING]

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def summary(self) -> str:
        error_count = len(self.errors)
        warning_count = len(self.warnings)
        status = "FAILED" if self.has_errors else "PASSED"
        return (
            f"DAG '{self.dag_name}' validation {status}: "
            f"{error_count} error(s), {warning_count} warning(s)"
        )

    def to_text(self) -> str:
        lines = [self.summary()]
        if self.entries:
            lines.append("")
            for entry in self.entries:
                lines.append(f"  {entry}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "dag_name": self.dag_name,
            "status": "failed" if self.has_errors else "passed",
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "entries": [
                {
                    "severity": e.severity.value,
                    "code": e.code,
                    "message": e.message,
                    "task_id": e.task_id,
                }
                for e in self.entries
            ],
        }
