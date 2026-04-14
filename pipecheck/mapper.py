"""DAG task dependency mapper — builds upstream/downstream maps for all tasks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipecheck.dag import DAG


class MapError(Exception):
    """Raised when mapping fails."""


@dataclass
class TaskMap:
    """Holds upstream and downstream task ID lists for a single task."""

    task_id: str
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        up = ", ".join(self.upstream) if self.upstream else "none"
        down = ", ".join(self.downstream) if self.downstream else "none"
        return f"{self.task_id}: upstream=[{up}] downstream=[{down}]"


@dataclass
class MapResult:
    """Full dependency map for every task in a DAG."""

    dag_name: str
    entries: Dict[str, TaskMap] = field(default_factory=dict)

    def get(self, task_id: str) -> TaskMap:
        if task_id not in self.entries:
            raise MapError(f"Task '{task_id}' not found in map")
        return self.entries[task_id]

    def __len__(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        lines = [f"Map for DAG '{self.dag_name}' ({len(self)} tasks):"]
        for entry in sorted(self.entries.values(), key=lambda e: e.task_id):
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGMapper:
    """Builds a full upstream/downstream dependency map for a DAG."""

    def map(self, dag: DAG) -> MapResult:
        result = MapResult(dag_name=dag.name)

        for task in dag.tasks.values():
            result.entries[task.task_id] = TaskMap(task_id=task.task_id)

        for parent_id, child_id in dag.edges:
            if parent_id in result.entries:
                result.entries[parent_id].downstream.append(child_id)
            if child_id in result.entries:
                result.entries[child_id].upstream.append(parent_id)

        for entry in result.entries.values():
            entry.upstream.sort()
            entry.downstream.sort()

        return result
