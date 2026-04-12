"""DAG routing: find all paths between two tasks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.dag import DAG


class RouteError(Exception):
    """Raised when routing cannot be performed."""


@dataclass
class Route:
    """A single path between two tasks."""

    task_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        return " -> ".join(self.task_ids)

    def __len__(self) -> int:
        return len(self.task_ids)


@dataclass
class RouteResult:
    """All routes found between source and target tasks."""

    dag_name: str
    source: str
    target: str
    routes: List[Route] = field(default_factory=list)

    def count(self) -> int:
        return len(self.routes)

    def has_routes(self) -> bool:
        return bool(self.routes)

    def shortest(self) -> Optional[Route]:
        if not self.routes:
            return None
        return min(self.routes, key=len)

    def longest(self) -> Optional[Route]:
        if not self.routes:
            return None
        return max(self.routes, key=len)

    def __str__(self) -> str:
        lines = [
            f"Routes in '{self.dag_name}': {self.source} -> {self.target}",
            f"  Found: {self.count()} path(s)",
        ]
        for i, route in enumerate(self.routes, 1):
            lines.append(f"  [{i}] {route}")
        return "\n".join(lines)


class DAGRouter:
    """Finds all paths between two tasks in a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def route(self, source: str, target: str) -> RouteResult:
        task_ids = {t.task_id for t in self._dag.tasks}
        if source not in task_ids:
            raise RouteError(f"Source task '{source}' not found in DAG.")
        if target not in task_ids:
            raise RouteError(f"Target task '{target}' not found in DAG.")

        routes: List[Route] = []
        self._dfs(source, target, [source], routes)
        return RouteResult(
            dag_name=self._dag.name,
            source=source,
            target=target,
            routes=routes,
        )

    def _dfs(
        self,
        current: str,
        target: str,
        path: List[str],
        routes: List[Route],
    ) -> None:
        if current == target and len(path) > 1:
            routes.append(Route(task_ids=list(path)))
            return
        for task in self._dag.tasks:
            if task.task_id in task.dependencies:
                continue
        for task in self._dag.tasks:
            if task.task_id == target and current in task.dependencies and current != target:
                routes.append(Route(task_ids=list(path) + ([target] if path[-1] != target else [])))
                return
        neighbors = [
            t.task_id
            for t in self._dag.tasks
            if current in t.dependencies and t.task_id not in path
        ]
        for neighbor in neighbors:
            self._dfs(neighbor, target, path + [neighbor], routes)
