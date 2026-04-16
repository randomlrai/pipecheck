"""DAG slimmer: removes redundant transitive edges from a DAG."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Set
from pipecheck.dag import DAG


class SlimError(Exception):
    pass


@dataclass
class SlimEntry:
    source: str
    target: str

    def __str__(self) -> str:
        return f"{self.source} -> {self.target} (redundant)"


@dataclass
class SlimResult:
    dag_name: str
    removed: List[SlimEntry] = field(default_factory=list)

    @property
    def has_removals(self) -> bool:
        return len(self.removed) > 0

    @property
    def count(self) -> int:
        return len(self.removed)

    def __str__(self) -> str:
        lines = [f"SlimResult({self.dag_name})"]
        if not self.removed:
            lines.append("  No redundant edges found.")
        else:
            for e in self.removed:
                lines.append(f"  {e}")
        return "\n".join(lines)


class DAGSlimmer:
    """Removes transitive redundant edges from a DAG."""

    def slim(self, dag: DAG) -> SlimResult:
        result = SlimResult(dag_name=dag.name)
        # Build adjacency
        task_ids = [t.task_id for t in dag.tasks]
        edges: Set[tuple] = set(dag.edges)

        def reachable_without(src: str, dst: str) -> bool:
            """BFS/DFS: can we reach dst from src without using the direct edge?"""
            visited: Set[str] = set()
            stack = [
                t for s, t in edges if s == src and t != dst
            ]
            while stack:
                node = stack.pop()
                if node == dst:
                    return True
                if node in visited:
                    continue
                visited.add(node)
                stack.extend(t for s, t in edges if s == node)
            return False

        for (src, dst) in list(edges):
            if reachable_without(src, dst):
                result.removed.append(SlimEntry(source=src, target=dst))

        return result
