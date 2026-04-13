"""DAG task indexer — builds a lookup index over task attributes."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class IndexError(Exception):  # noqa: A001
    """Raised when indexing fails."""


@dataclass
class IndexEntry:
    task_id: str
    attribute: str
    value: str

    def __str__(self) -> str:
        return f"{self.task_id}: {self.attribute}={self.value}"


@dataclass
class IndexResult:
    dag_name: str
    entries: List[IndexEntry] = field(default_factory=list)

    def has_entries(self) -> bool:
        return len(self.entries) > 0

    def count(self) -> int:
        return len(self.entries)

    def tasks_for_value(self, value: str) -> List[str]:
        return [e.task_id for e in self.entries if e.value == value]

    def __str__(self) -> str:
        if not self.entries:
            return f"Index for '{self.dag_name}': no entries"
        lines = [f"Index for '{self.dag_name}' ({self.count()} entries):"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGIndexer:
    """Builds a searchable index over a DAG's task metadata."""

    SUPPORTED_ATTRIBUTES = {"owner", "team", "env", "version"}

    def index(self, dag: DAG, attribute: str) -> IndexResult:
        if attribute not in self.SUPPORTED_ATTRIBUTES:
            raise IndexError(
                f"Unsupported attribute '{attribute}'. "
                f"Choose from: {sorted(self.SUPPORTED_ATTRIBUTES)}"
            )
        result = IndexResult(dag_name=dag.name)
        for task in dag.tasks.values():
            value: Optional[str] = task.metadata.get(attribute)
            if value is not None:
                result.entries.append(
                    IndexEntry(task_id=task.task_id, attribute=attribute, value=str(value))
                )
        result.entries.sort(key=lambda e: (e.value, e.task_id))
        return result
