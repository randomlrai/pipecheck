"""Topological sort utilities for DAG task ordering."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG


class SortError(Exception):
    """Raised when a topological sort cannot be completed."""


@dataclass
class SortResult:
    """Result of a topological sort operation."""

    dag_name: str
    order: List[str] = field(default_factory=list)

    def __str__(self) -> str:  # pragma: no cover
        lines = [f"Topological order for DAG '{self.dag_name}':"]  
        for i, task_id in enumerate(self.order, 1):
            lines.append(f"  {i:>3}. {task_id}")
        return "\n".join(lines)

    @property
    def count(self) -> int:
        return len(self.order)


class DAGSorter:
    """Performs topological sorting of a DAG using Kahn's algorithm."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def sort(self) -> SortResult:
        """Return tasks in a valid topological execution order.

        Raises:
            SortError: if the DAG contains a cycle and cannot be sorted.
        """
        in_degree: dict[str, int] = {t.task_id: 0 for t in self._dag.tasks}
        adjacency: dict[str, list[str]] = {t.task_id: [] for t in self._dag.tasks}

        for task in self._dag.tasks:
            for dep in task.dependencies:
                if dep not in adjacency:
                    raise SortError(
                        f"Task '{task.task_id}' depends on unknown task '{dep}'."
                    )
                adjacency[dep].append(task.task_id)
                in_degree[task.task_id] += 1

        queue: deque[str] = deque(
            tid for tid, deg in sorted(in_degree.items()) if deg == 0
        )
        order: list[str] = []

        while queue:
            tid = queue.popleft()
            order.append(tid)
            for neighbour in sorted(adjacency[tid]):
                in_degree[neighbour] -= 1
                if in_degree[neighbour] == 0:
                    queue.append(neighbour)

        if len(order) != len(in_degree):
            raise SortError(
                "Topological sort failed: the DAG contains a cycle."
            )

        return SortResult(dag_name=self._dag.name, order=order)
