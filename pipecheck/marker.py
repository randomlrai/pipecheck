"""DAG task marker — attach run-time markers (skip, force-run, deprecated) to tasks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

VALID_MARKERS = {"skip", "force", "deprecated", "experimental"}


class MarkerError(Exception):
    """Raised when an invalid marker operation is attempted."""


@dataclass
class MarkerEntry:
    task_id: str
    marker: str
    reason: Optional[str] = None

    def __str__(self) -> str:
        base = f"[{self.marker.upper()}] {self.task_id}"
        if self.reason:
            base += f" — {self.reason}"
        return base


@dataclass
class MarkerResult:
    dag_name: str
    entries: List[MarkerEntry] = field(default_factory=list)

    @property
    def has_markers(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def by_marker(self, marker: str) -> List[MarkerEntry]:
        return [e for e in self.entries if e.marker == marker]

    def __str__(self) -> str:
        if not self.entries:
            return f"MarkerResult({self.dag_name}): no markers applied"
        lines = [f"MarkerResult({self.dag_name}):{self.count} marker(s)"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGMarker:
    """Applies named markers to tasks within a DAG."""

    def __init__(self, dag) -> None:
        self._dag = dag
        self._markers: Dict[str, MarkerEntry] = {}

    def mark(self, task_id: str, marker: str, reason: Optional[str] = None) -> MarkerEntry:
        if marker not in VALID_MARKERS:
            raise MarkerError(
                f"Unknown marker '{marker}'. Valid markers: {sorted(VALID_MARKERS)}"
            )
        task_ids = {t.task_id for t in self._dag.tasks}
        if task_id not in task_ids:
            raise MarkerError(f"Task '{task_id}' not found in DAG '{self._dag.name}'")
        entry = MarkerEntry(task_id=task_id, marker=marker, reason=reason)
        self._markers[task_id] = entry
        return entry

    def unmark(self, task_id: str) -> None:
        if task_id not in self._markers:
            raise MarkerError(f"Task '{task_id}' has no marker to remove")
        del self._markers[task_id]

    def build(self) -> MarkerResult:
        result = MarkerResult(dag_name=self._dag.name)
        result.entries = list(self._markers.values())
        return result
