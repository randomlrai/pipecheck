"""DAG and task naming utilities for pipecheck."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG


class NameError(Exception):  # noqa: A001
    """Raised when a naming operation fails."""


@dataclass
class NameEntry:
    task_id: str
    old_name: str
    new_name: str

    def __str__(self) -> str:
        return f"{self.task_id}: '{self.old_name}' -> '{self.new_name}'"


@dataclass
class NameResult:
    dag_name: str
    entries: List[NameEntry] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        lines = [f"NameResult for '{self.dag_name}':"]
        if not self.entries:
            lines.append("  (no changes)")
        else:
            for entry in self.entries:
                lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGNamer:
    """Applies a naming convention transform to task display names."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def apply(self, mapping: dict[str, str]) -> NameResult:
        """Return a NameResult reflecting the proposed renames in *mapping*.

        *mapping* is {task_id: new_display_name}.  Tasks not in the mapping
        are left untouched.  Raises NameError if a task_id is unknown.
        """
        task_ids = {t.task_id for t in self._dag.tasks}
        for tid in mapping:
            if tid not in task_ids:
                raise NameError(f"Unknown task id: '{tid}'")

        result = NameResult(dag_name=self._dag.name)
        for task in sorted(self._dag.tasks, key=lambda t: t.task_id):
            if task.task_id in mapping:
                new_name = mapping[task.task_id]
                old_name = task.metadata.get("display_name", task.task_id)
                if new_name != old_name:
                    result.entries.append(
                        NameEntry(
                            task_id=task.task_id,
                            old_name=old_name,
                            new_name=new_name,
                        )
                    )
        return result
