"""DAG task anchoring — mark tasks as fixed reference points."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class AnchorError(Exception):
    """Raised when an anchoring operation fails."""


@dataclass
class AnchorEntry:
    task_id: str
    reason: Optional[str] = None
    pinned_version: Optional[str] = None

    def __str__(self) -> str:
        parts = [f"[ANCHOR] {self.task_id}"]
        if self.pinned_version:
            parts.append(f"v{self.pinned_version}")
        if self.reason:
            parts.append(f"— {self.reason}")
        return " ".join(parts)


@dataclass
class AnchorResult:
    dag_name: str
    anchors: List[AnchorEntry] = field(default_factory=list)

    def has_anchors(self) -> bool:
        return len(self.anchors) > 0

    def count(self) -> int:
        return len(self.anchors)

    def task_ids(self) -> List[str]:
        return [a.task_id for a in self.anchors]

    def __str__(self) -> str:
        if not self.anchors:
            return f"AnchorResult({self.dag_name}): no anchors"
        lines = [f"AnchorResult({self.dag_name}): {self.count()} anchor(s)"]
        for entry in self.anchors:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGAnchorer:
    """Attach anchor metadata to tasks in a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag
        self._anchors: Dict[str, AnchorEntry] = {}

    def anchor(
        self,
        task_id: str,
        reason: Optional[str] = None,
        pinned_version: Optional[str] = None,
    ) -> AnchorEntry:
        if task_id not in self._dag.tasks:
            raise AnchorError(f"Task '{task_id}' not found in DAG '{self._dag.name}'")
        entry = AnchorEntry(task_id=task_id, reason=reason, pinned_version=pinned_version)
        self._anchors[task_id] = entry
        return entry

    def release(self, task_id: str) -> None:
        if task_id not in self._anchors:
            raise AnchorError(f"Task '{task_id}' is not anchored")
        del self._anchors[task_id]

    def build_result(self) -> AnchorResult:
        return AnchorResult(
            dag_name=self._dag.name,
            anchors=list(self._anchors.values()),
        )

    def is_anchored(self, task_id: str) -> bool:
        return task_id in self._anchors
