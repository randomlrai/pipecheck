"""DAG graph metrics and reachability analysis."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set
from pipecheck.dag import DAG


class GraphError(Exception):
    """Raised when graph analysis fails."""


@dataclass
class GraphMetrics:
    dag_name: str
    node_count: int
    edge_count: int
    max_depth: int
    root_tasks: List[str]
    leaf_tasks: List[str]
    reachable_from: Dict[str, Set[str]] = field(default_factory=dict)

    def __str__(self) -> str:
        roots = ", ".join(sorted(self.root_tasks)) or "none"
        leaves = ", ".join(sorted(self.leaf_tasks)) or "none"
        return (
            f"GraphMetrics({self.dag_name}): "
            f"{self.node_count} nodes, {self.edge_count} edges, "
            f"depth={self.max_depth}, roots=[{roots}], leaves=[{leaves}]"
        )


class DAGGrapher:
    """Compute structural graph metrics for a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def _build_adjacency(self) -> Dict[str, List[str]]:
        adj: Dict[str, List[str]] = {t.task_id: [] for t in self._dag.tasks}
        for task in self._dag.tasks:
            for dep in task.dependencies:
                adj[dep].append(task.task_id)
        return adj

    def _compute_depth(self, adj: Dict[str, List[str]], roots: List[str]) -> int:
        if not roots:
            return 0
        max_d = 0
        visited: Dict[str, int] = {}

        def dfs(node: str, depth: int) -> None:
            nonlocal max_d
            if node in visited and visited[node] >= depth:
                return
            visited[node] = depth
            max_d = max(max_d, depth)
            for child in adj.get(node, []):
                dfs(child, depth + 1)

        for r in roots:
            dfs(r, 0)
        return max_d

    def _reachable(self, adj: Dict[str, List[str]], start: str) -> Set[str]:
        visited: Set[str] = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            stack.extend(adj.get(node, []))
        visited.discard(start)
        return visited

    def analyze(self) -> GraphMetrics:
        tasks = list(self._dag.tasks)
        if not tasks:
            return GraphMetrics(self._dag.name, 0, 0, 0, [], [])

        all_ids = {t.task_id for t in tasks}
        dep_ids = {dep for t in tasks for dep in t.dependencies}
        edge_count = sum(len(t.dependencies) for t in tasks)

        roots = sorted(all_ids - dep_ids)
        leaves = sorted(
            t.task_id for t in tasks if not any(
                t.task_id in other.dependencies for other in tasks
            )
        )

        adj = self._build_adjacency()
        max_depth = self._compute_depth(adj, roots)
        reachable = {tid: self._reachable(adj, tid) for tid in all_ids}

        return GraphMetrics(
            dag_name=self._dag.name,
            node_count=len(all_ids),
            edge_count=edge_count,
            max_depth=max_depth,
            root_tasks=roots,
            leaf_tasks=leaves,
            reachable_from=reachable,
        )
