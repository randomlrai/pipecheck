"""DAG splitter: partition a DAG into independent sub-DAGs by connected components."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG, Task


class SplitError(Exception):
    """Raised when a DAG cannot be split as requested."""


@dataclass
class SplitResult:
    """Holds the sub-DAGs produced by splitting."""

    source_name: str
    parts: List[DAG] = field(default_factory=list)

    def __str__(self) -> str:
        n = len(self.parts)
        return (
            f"SplitResult(source={self.source_name!r}, parts={n})"
        )

    @property
    def count(self) -> int:
        return len(self.parts)


class DAGSplitter:
    """Splits a DAG into weakly-connected-component sub-DAGs."""

    def split(self, dag: DAG) -> SplitResult:
        """Return a SplitResult whose parts are the connected components of *dag*."""
        if not dag.tasks:
            raise SplitError("Cannot split an empty DAG.")

        task_ids = list(dag.tasks.keys())
        adjacency: dict[str, set[str]] = {tid: set() for tid in task_ids}

        for src, targets in dag.dependencies.items():
            for tgt in targets:
                adjacency[src].add(tgt)
                adjacency[tgt].add(src)

        visited: set[str] = set()
        components: list[list[str]] = []

        for tid in task_ids:
            if tid in visited:
                continue
            component: list[str] = []
            stack = [tid]
            while stack:
                node = stack.pop()
                if node in visited:
                    continue
                visited.add(node)
                component.append(node)
                stack.extend(adjacency[node] - visited)
            components.append(component)

        parts: list[DAG] = []
        for idx, component in enumerate(components):
            sub = DAG(name=f"{dag.name}_part{idx + 1}")
            component_set = set(component)
            for tid in component:
                sub.add_task(dag.tasks[tid])
            for src in component:
                for tgt in dag.dependencies.get(src, []):
                    if tgt in component_set:
                        sub.add_dependency(src, tgt)
            parts.append(sub)

        return SplitResult(source_name=dag.name, parts=parts)
