"""DAG crawler: traverse all tasks in dependency order with visit tracking."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG, Task


class CrawlError(Exception):
    """Raised when a crawl operation fails."""


@dataclass
class CrawlStep:
    task_id: str
    depth: int
    parent_id: Optional[str] = None

    def __str__(self) -> str:
        indent = "  " * self.depth
        parent_info = f" (parent: {self.parent_id})" if self.parent_id else ""
        return f"{indent}{self.task_id}{parent_info}"


@dataclass
class CrawlResult:
    dag_name: str
    steps: List[CrawlStep] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.steps)

    @property
    def task_ids(self) -> List[str]:
        return [s.task_id for s in self.steps]

    @property
    def max_depth(self) -> int:
        if not self.steps:
            return 0
        return max(s.depth for s in self.steps)

    def __str__(self) -> str:
        lines = [f"Crawl of '{self.dag_name}': {self.count} task(s), max depth {self.max_depth}"]
        for step in self.steps:
            lines.append(str(step))
        return "\n".join(lines)


class DAGCrawler:
    """Breadth-first crawler over a DAG."""

    def crawl(self, dag: DAG, start_id: Optional[str] = None) -> CrawlResult:
        """Crawl the DAG from roots (or a specific start task) in BFS order."""
        if start_id is not None and start_id not in dag.tasks:
            raise CrawlError(f"Task '{start_id}' not found in DAG '{dag.name}'")

        result = CrawlResult(dag_name=dag.name)
        visited = set()

        # Build children map
        children: dict = {tid: [] for tid in dag.tasks}
        for parent, child in dag.edges:
            children[parent].append(child)

        if start_id:
            queue = [(start_id, 0, None)]
        else:
            # Start from root tasks (no incoming edges)
            has_parent = {child for _, child in dag.edges}
            roots = [tid for tid in dag.tasks if tid not in has_parent]
            queue = [(r, 0, None) for r in sorted(roots)]

        while queue:
            task_id, depth, parent_id = queue.pop(0)
            if task_id in visited:
                continue
            visited.add(task_id)
            result.steps.append(CrawlStep(task_id=task_id, depth=depth, parent_id=parent_id))
            for child in sorted(children.get(task_id, [])):
                if child not in visited:
                    queue.append((child, depth + 1, task_id))

        return result
