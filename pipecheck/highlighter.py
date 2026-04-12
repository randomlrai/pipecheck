"""Highlight specific tasks in a DAG based on criteria."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG


class HighlightError(Exception):
    """Raised when highlighting fails."""


@dataclass
class HighlightEntry:
    task_id: str
    reason: str
    color: str = "yellow"

    def __str__(self) -> str:
        return f"[{self.color}] {self.task_id}: {self.reason}"


@dataclass
class HighlightResult:
    dag_name: str
    entries: List[HighlightEntry] = field(default_factory=list)

    def has_highlights(self) -> bool:
        return len(self.entries) > 0

    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.entries:
            return f"No highlights for DAG '{self.dag_name}'."
        lines = [f"Highlights for DAG '{self.dag_name}':"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGHighlighter:
    """Highlight tasks in a DAG by tag, prefix, or custom criteria."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def highlight_by_tag(self, tag: str, color: str = "yellow") -> HighlightResult:
        """Highlight all tasks that carry a given tag."""
        result = HighlightResult(dag_name=self._dag.name)
        for task in self._dag.tasks.values():
            tags = task.metadata.get("tags", [])
            if tag in tags:
                result.entries.append(
                    HighlightEntry(task_id=task.task_id, reason=f"tag={tag}", color=color)
                )
        return result

    def highlight_by_prefix(self, prefix: str, color: str = "cyan") -> HighlightResult:
        """Highlight all tasks whose ID starts with the given prefix."""
        result = HighlightResult(dag_name=self._dag.name)
        for task_id in self._dag.tasks:
            if task_id.startswith(prefix):
                result.entries.append(
                    HighlightEntry(
                        task_id=task_id,
                        reason=f"prefix={prefix}",
                        color=color,
                    )
                )
        return result

    def highlight_by_ids(
        self, task_ids: List[str], reason: str = "manual", color: str = "red"
    ) -> HighlightResult:
        """Highlight an explicit list of task IDs."""
        result = HighlightResult(dag_name=self._dag.name)
        for task_id in task_ids:
            if task_id not in self._dag.tasks:
                raise HighlightError(f"Task '{task_id}' not found in DAG '{self._dag.name}'.")
            result.entries.append(
                HighlightEntry(task_id=task_id, reason=reason, color=color)
            )
        return result
