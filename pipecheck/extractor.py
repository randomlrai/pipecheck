"""Extract a subgraph from a DAG given a set of task IDs."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Set
from pipecheck.dag import DAG, Task


class ExtractError(Exception):
    """Raised when extraction fails."""


@dataclass
class ExtractResult:
    dag_name: str
    task_ids: List[str] = field(default_factory=list)
    skipped_ids: List[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.task_ids)

    @property
    def has_skipped(self) -> bool:
        return len(self.skipped_ids) > 0

    def __str__(self) -> str:
        lines = [f"Extract result for '{self.dag_name}':"]
        if self.task_ids:
            lines.append(f"  extracted ({self.count}): " + ", ".join(sorted(self.task_ids)))
        else:
            lines.append("  extracted (0): none")
        if self.skipped_ids:
            lines.append("  skipped: " + ", ".join(sorted(self.skipped_ids)))
        return "\n".join(lines)


class DAGExtractor:
    """Extract a subgraph containing only the requested task IDs and their edges."""

    def extract(self, dag: DAG, task_ids: List[str]) -> ExtractResult:
        if not task_ids:
            raise ExtractError("task_ids must not be empty")

        requested: Set[str] = set(task_ids)
        existing: Set[str] = {t.task_id for t in dag.tasks}
        found = requested & existing
        skipped = requested - existing

        new_dag = DAG(name=dag.name)
        for task in dag.tasks:
            if task.task_id in found:
                new_dag.add_task(task)

        for src, dst in dag.edges:
            if src in found and dst in found:
                new_dag.add_edge(src, dst)

        return ExtractResult(
            dag_name=dag.name,
            task_ids=list(found),
            skipped_ids=list(skipped),
        )
