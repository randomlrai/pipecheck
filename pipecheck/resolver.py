"""Resolve task execution order using topological sort."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG


class ResolveError(Exception):
    """Raised when resolution fails (e.g. cycle detected)."""


@dataclass
class ResolveResult:
    dag_name: str
    order: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        if not self.order:
            return f"ResolveResult({self.dag_name}): empty"
        steps = " -> ".join(self.order)
        return f"ResolveResult({self.dag_name}): {steps}"

    @property
    def count(self) -> int:
        return len(self.order)


class DAGResolver:
    """Produce a deterministic topological ordering for a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def resolve(self) -> ResolveResult:
        """Return tasks in topological order (Kahn's algorithm).

        Raises ResolveError if a cycle is detected.
        """
        dag = self._dag
        in_degree: dict[str, int] = {t.task_id: 0 for t in dag.tasks}
        adjacency: dict[str, list[str]] = {t.task_id: [] for t in dag.tasks}

        for src, dst in dag.edges:
            adjacency[src].append(dst)
            in_degree[dst] += 1

        queue = sorted(
            [tid for tid, deg in in_degree.items() if deg == 0]
        )
        order: list[str] = []

        while queue:
            node = queue.pop(0)
            order.append(node)
            for neighbour in sorted(adjacency[node]):
                in_degree[neighbour] -= 1
                if in_degree[neighbour] == 0:
                    queue.append(neighbour)

        if len(order) != len(dag.tasks):
            raise ResolveError(
                f"Cycle detected in DAG '{dag.name}'; cannot resolve order."
            )

        return ResolveResult(dag_name=dag.name, order=order)
