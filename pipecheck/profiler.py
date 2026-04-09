"""DAG profiling: estimates runtime and flags bottlenecks."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG, Task


@dataclass
class TaskProfile:
    task_id: str
    timeout: Optional[int]
    depth: int
    dependents: int
    is_bottleneck: bool = False

    def __str__(self) -> str:
        flags = " [BOTTLENECK]" if self.is_bottleneck else ""
        return (
            f"TaskProfile({self.task_id}, depth={self.depth}, "
            f"dependents={self.dependents}, timeout={self.timeout}){flags}"
        )


@dataclass
class DAGProfile:
    total_tasks: int
    max_depth: int
    critical_path: List[str]
    bottlenecks: List[str]
    task_profiles: Dict[str, TaskProfile] = field(default_factory=dict)

    def summary(self) -> str:
        lines = [
            f"Tasks      : {self.total_tasks}",
            f"Max depth  : {self.max_depth}",
            f"Critical   : {' -> '.join(self.critical_path)}",
            f"Bottlenecks: {', '.join(self.bottlenecks) or 'none'}",
        ]
        return "\n".join(lines)


class DAGProfiler:
    """Analyses a DAG and produces a DAGProfile."""

    BOTTLENECK_DEPENDENT_THRESHOLD = 2

    def __init__(self, dag: DAG) -> None:
        self.dag = dag

    def _compute_depths(self) -> Dict[str, int]:
        depths: Dict[str, int] = {}

        def dfs(task_id: str) -> int:
            if task_id in depths:
                return depths[task_id]
            task = self.dag.tasks[task_id]
            if not task.dependencies:
                depths[task_id] = 0
            else:
                depths[task_id] = 1 + max(dfs(dep) for dep in task.dependencies)
            return depths[task_id]

        for tid in self.dag.tasks:
            dfs(tid)
        return depths

    def _compute_dependents(self) -> Dict[str, int]:
        counts: Dict[str, int] = {tid: 0 for tid in self.dag.tasks}
        for task in self.dag.tasks.values():
            for dep in task.dependencies:
                counts[dep] += 1
        return counts

    def _critical_path(self, depths: Dict[str, int]) -> List[str]:
        if not depths:
            return []
        deepest = max(depths, key=lambda t: depths[t])
        path = [deepest]
        current = self.dag.tasks[deepest]
        while current.dependencies:
            parent = max(
                current.dependencies, key=lambda t: depths.get(t, 0)
            )
            path.append(parent)
            current = self.dag.tasks[parent]
        return list(reversed(path))

    def profile(self) -> DAGProfile:
        depths = self._compute_depths()
        dependents = self._compute_dependents()
        critical = self._critical_path(depths)
        profiles: Dict[str, TaskProfile] = {}
        bottlenecks: List[str] = []

        for tid, task in self.dag.tasks.items():
            is_bn = dependents[tid] >= self.BOTTLENECK_DEPENDENT_THRESHOLD
            if is_bn:
                bottlenecks.append(tid)
            profiles[tid] = TaskProfile(
                task_id=tid,
                timeout=task.metadata.get("timeout"),
                depth=depths.get(tid, 0),
                dependents=dependents[tid],
                is_bottleneck=is_bn,
            )

        return DAGProfile(
            total_tasks=len(self.dag.tasks),
            max_depth=max(depths.values(), default=0),
            critical_path=critical,
            bottlenecks=bottlenecks,
            task_profiles=profiles,
        )
