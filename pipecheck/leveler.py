"""Assigns depth levels to tasks in a DAG based on topological distance from roots."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List
from pipecheck.dag import DAG


class LevelError(Exception):
    """Raised when leveling cannot be completed."""


@dataclass
class LevelEntry:
    task_id: str
    depth: int

    def __str__(self) -> str:
        indent = "  " * self.depth
        return f"{indent}[{self.depth}] {self.task_id}"


@dataclass
class LevelResult:
    dag_name: str
    entries: List[LevelEntry] = field(default_factory=list)

    @property
    def max_depth(self) -> int:
        if not self.entries:
            return 0
        return max(e.depth for e in self.entries)

    @property
    def count(self) -> int:
        return len(self.entries)

    def at_depth(self, depth: int) -> List[LevelEntry]:
        return [e for e in self.entries if e.depth == depth]

    def __str__(self) -> str:
        lines = [f"DAG: {self.dag_name}", f"Max depth: {self.max_depth}"]
        for entry in sorted(self.entries, key=lambda e: (e.depth, e.task_id)):
            lines.append(str(entry))
        return "\n".join(lines)


class DAGLeveler:
    """Computes topological depth levels for all tasks in a DAG."""

    def level(self, dag: DAG) -> LevelResult:
        tasks = {t.task_id: t for t in dag.tasks}
        if not tasks:
            return LevelResult(dag_name=dag.name)

        in_edges: Dict[str, List[str]] = {tid: [] for tid in tasks}
        for task in dag.tasks:
            for dep in task.dependencies:
                if dep not in tasks:
                    raise LevelError(
                        f"Task '{task.task_id}' depends on unknown task '{dep}'"
                    )
                in_edges[task.task_id].append(dep)

        depths: Dict[str, int] = {}
        visited = set()

        def _dfs(tid: str, stack: set) -> int:
            if tid in stack:
                raise LevelError(f"Cycle detected involving task '{tid}'")
            if tid in depths:
                return depths[tid]
            stack.add(tid)
            dep_depths = [_dfs(dep, stack) for dep in in_edges[tid]]
            depths[tid] = (max(dep_depths) + 1) if dep_depths else 0
            stack.discard(tid)
            visited.add(tid)
            return depths[tid]

        for tid in tasks:
            if tid not in visited:
                _dfs(tid, set())

        entries = [LevelEntry(task_id=tid, depth=d) for tid, d in depths.items()]
        return LevelResult(dag_name=dag.name, entries=entries)
