"""Cycle detection and reporting for DAGs."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG


class CycleError(Exception):
    """Raised when cycle detection encounters an invalid state."""


@dataclass
class CycleResult:
    """Result of cycle detection on a DAG."""

    dag_name: str
    cycles: List[List[str]] = field(default_factory=list)

    @property
    def has_cycles(self) -> bool:
        return len(self.cycles) > 0

    @property
    def count(self) -> int:
        return len(self.cycles)

    def __str__(self) -> str:
        lines = [f"CycleResult for '{self.dag_name}'"]
        if not self.has_cycles:
            lines.append("  No cycles detected.")
        else:
            lines.append(f"  {self.count} cycle(s) found:")
            for i, cycle in enumerate(self.cycles, 1):
                path = " -> ".join(cycle)
                lines.append(f"  [{i}] {path}")
        return "\n".join(lines)


class DAGCycler:
    """Detects all cycles in a DAG using DFS."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def detect(self) -> CycleResult:
        visited: set = set()
        rec_stack: set = set()
        cycles: List[List[str]] = []

        def _build_adj():
            adj = {t.task_id: [] for t in self._dag.tasks}
            for src, dst in self._dag.edges:
                adj[src].append(dst)
            return adj

        adj = _build_adj()

        def dfs(node: str, path: List[str]) -> None:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for neighbour in adj.get(node, []):
                if neighbour not in visited:
                    dfs(neighbour, path)
                elif neighbour in rec_stack:
                    cycle_start = path.index(neighbour)
                    cycle = path[cycle_start:] + [neighbour]
                    if cycle not in cycles:
                        cycles.append(cycle)
            path.pop()
            rec_stack.discard(node)

        for task in self._dag.tasks:
            if task.task_id not in visited:
                dfs(task.task_id, [])

        return CycleResult(dag_name=self._dag.name, cycles=cycles)
