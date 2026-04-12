"""Normalize task IDs and edge definitions within a DAG."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG, Task


class NormalizeError(Exception):
    """Raised when normalization cannot be completed."""


@dataclass
class NormalizeEntry:
    task_id: str
    original: str
    normalized: str

    def __str__(self) -> str:
        return f"{self.task_id}: '{self.original}' -> '{self.normalized}'"


@dataclass
class NormalizeResult:
    dag_name: str
    entries: List[NormalizeEntry] = field(default_factory=list)
    dag: DAG = field(default=None, repr=False)

    @property
    def has_changes(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.entries:
            return f"[{self.dag_name}] No normalization changes."
        lines = [f"[{self.dag_name}] {self.count} task(s) normalized:"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGNormalizer:
    """Normalize task IDs to snake_case and strip whitespace."""

    def normalize(self, dag: DAG) -> NormalizeResult:
        result = NormalizeResult(dag_name=dag.name)
        new_dag = DAG(name=dag.name)
        id_map: dict[str, str] = {}

        for task in dag.tasks.values():
            normalized_id = self._normalize_id(task.task_id)
            id_map[task.task_id] = normalized_id
            if normalized_id != task.task_id:
                result.entries.append(
                    NormalizeEntry(
                        task_id=task.task_id,
                        original=task.task_id,
                        normalized=normalized_id,
                    )
                )
            new_task = Task(
                task_id=normalized_id,
                description=task.description,
                timeout=task.timeout,
                metadata=dict(task.metadata),
            )
            new_dag.add_task(new_task)

        for src, dst in dag.edges:
            new_dag.add_edge(id_map[src], id_map[dst])

        result.dag = new_dag
        return result

    @staticmethod
    def _normalize_id(task_id: str) -> str:
        return task_id.strip().lower().replace(" ", "_").replace("-", "_")
