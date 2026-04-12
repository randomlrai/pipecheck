"""Reachability analysis for DAG tasks."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Set

from pipecheck.dag import DAG


class ReachabilityError(Exception):
    """Raised when reachability analysis fails."""


@dataclass
class ReachabilityResult:
    """Holds reachability information for every task in a DAG."""

    dag_name: str
    reachable: Dict[str, FrozenSet[str]] = field(default_factory=dict)

    def can_reach(self, source: str, target: str) -> bool:
        """Return True if *source* can reach *target*."""
        return target in self.reachable.get(source, frozenset())

    def unreachable_from(self, source: str) -> FrozenSet[str]:
        """Return all task ids that *source* cannot reach."""
        all_ids: Set[str] = set(self.reachable.keys())
        return frozenset(all_ids - self.reachable.get(source, frozenset()) - {source})

    def __str__(self) -> str:
        lines: List[str] = [f"ReachabilityResult(dag={self.dag_name!r})"]
        for task_id in sorted(self.reachable):
            targets = ", ".join(sorted(self.reachable[task_id])) or "(none)"
            lines.append(f"  {task_id} -> {targets}")
        return "\n".join(lines)


class DAGReachabilityAnalyzer:
    """Computes transitive reachability for all tasks in a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def analyze(self) -> ReachabilityResult:
        """Run BFS/DFS from every task and return a ReachabilityResult."""
        if not self._dag.tasks:
            raise ReachabilityError("DAG has no tasks to analyze.")

        adjacency: Dict[str, List[str]] = {t: [] for t in self._dag.tasks}
        for src, dst in self._dag.edges:
            adjacency[src].append(dst)

        reachable: Dict[str, FrozenSet[str]] = {}
        for start in self._dag.tasks:
            visited: Set[str] = set()
            stack = list(adjacency[start])
            while stack:
                node = stack.pop()
                if node in visited:
                    continue
                visited.add(node)
                stack.extend(adjacency.get(node, []))
            reachable[start] = frozenset(visited)

        return ReachabilityResult(dag_name=self._dag.name, reachable=reachable)
