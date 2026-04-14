"""Critical path tracer: finds the longest weighted path through a DAG."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG, Task


class CriticalPathError(Exception):
    """Raised when critical path tracing fails."""


@dataclass
class PathStep:
    task_id: str
    weight: float
    cumulative: float

    def __str__(self) -> str:
        marker = " [*]" if self.weight > 0 else ""
        return f"{self.task_id} (weight={self.weight:.2f}, cumulative={self.cumulative:.2f}){marker}"


@dataclass
class CriticalPath:
    dag_name: str
    steps: List[PathStep] = field(default_factory=list)
    total_weight: float = 0.0

    def __str__(self) -> str:
        lines = [f"Critical path for '{self.dag_name}' (total={self.total_weight:.2f}):"]
        for i, step in enumerate(self.steps):
            prefix = "  ->" if i > 0 else "    "
            lines.append(f"{prefix} {step}")
        if not self.steps:
            lines.append("  (empty)")
        return "\n".join(lines)

    @property
    def task_ids(self) -> List[str]:
        return [s.task_id for s in self.steps]

    def length(self) -> int:
        return len(self.steps)


class CriticalPathTracer:
    """Traces the critical (longest weighted) path through a DAG."""

    def trace(self, dag: DAG, weight_attr: str = "timeout") -> CriticalPath:
        tasks = dag.tasks
        if not tasks:
            return CriticalPath(dag_name=dag.name)

        # Build adjacency and in-degree
        children: dict[str, list[str]] = {t: [] for t in tasks}
        parents: dict[str, list[str]] = {t: [] for t in tasks}
        for src, dst in dag.edges:
            children[src].append(dst)
            parents[dst].append(src)

        def _weight(task_id: str) -> float:
            task: Task = tasks[task_id]
            val = task.metadata.get(weight_attr, 0)
            try:
                return float(val)
            except (TypeError, ValueError):
                return 0.0

        # Topological sort (Kahn's algorithm)
        in_deg = {t: len(parents[t]) for t in tasks}
        queue = [t for t in tasks if in_deg[t] == 0]
        topo: list[str] = []
        while queue:
            node = queue.pop(0)
            topo.append(node)
            for child in children[node]:
                in_deg[child] -= 1
                if in_deg[child] == 0:
                    queue.append(child)

        if len(topo) != len(tasks):
            raise CriticalPathError("DAG contains a cycle; cannot trace critical path.")

        # DP for longest path
        dist: dict[str, float] = {t: _weight(t) for t in tasks}
        prev: dict[str, Optional[str]] = {t: None for t in tasks}
        for node in topo:
            for child in children[node]:
                candidate = dist[node] + _weight(child)
                if candidate > dist[child]:
                    dist[child] = candidate
                    prev[child] = node

        # Find end of critical path
        end = max(dist, key=lambda t: dist[t])
        path_ids: list[str] = []
        cur: Optional[str] = end
        while cur is not None:
            path_ids.append(cur)
            cur = prev[cur]
        path_ids.reverse()

        cumulative = 0.0
        steps = []
        for tid in path_ids:
            w = _weight(tid)
            cumulative += w
            steps.append(PathStep(task_id=tid, weight=w, cumulative=cumulative))

        return CriticalPath(dag_name=dag.name, steps=steps, total_weight=cumulative)
