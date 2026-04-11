"""DAG task labeler — attach and query display labels on tasks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class LabelError(Exception):
    """Raised when a labeling operation fails."""


@dataclass
class LabelEntry:
    task_id: str
    label: str
    color: Optional[str] = None

    def __str__(self) -> str:
        base = f"{self.task_id} -> \"{self.label}\""
        if self.color:
            base += f" [{self.color}]"
        return base


@dataclass
class LabelResult:
    dag_name: str
    entries: List[LabelEntry] = field(default_factory=list)

    @property
    def has_labels(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def get(self, task_id: str) -> Optional[LabelEntry]:
        for e in self.entries:
            if e.task_id == task_id:
                return e
        return None

    def __str__(self) -> str:
        if not self.entries:
            return f"LabelResult({self.dag_name}): no labels"
        lines = [f"LabelResult({self.dag_name}): {self.count} label(s)"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGLabeler:
    """Attach human-readable display labels to DAG tasks."""

    def label(
        self,
        dag: DAG,
        label_map: Dict[str, str],
        color_map: Optional[Dict[str, str]] = None,
    ) -> LabelResult:
        """Apply labels (and optional colors) to tasks in *dag*.

        Args:
            dag: The DAG whose tasks are to be labeled.
            label_map: Mapping of task_id -> display label string.
            color_map: Optional mapping of task_id -> color hint string.

        Returns:
            A :class:`LabelResult` containing one entry per labeled task.

        Raises:
            LabelError: If a task_id in *label_map* does not exist in *dag*.
        """
        color_map = color_map or {}
        task_ids = {t.task_id for t in dag.tasks}

        for tid in label_map:
            if tid not in task_ids:
                raise LabelError(
                    f"Task '{tid}' not found in DAG '{dag.name}'."
                )

        entries = [
            LabelEntry(
                task_id=tid,
                label=label,
                color=color_map.get(tid),
            )
            for tid, label in sorted(label_map.items())
        ]
        return LabelResult(dag_name=dag.name, entries=entries)
