"""DAG inspector: surface per-task metadata and dependency statistics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG, Task


@dataclass
class TaskInspection:
    """Inspection result for a single task."""

    task_id: str
    description: Optional[str]
    timeout: Optional[int]
    tags: List[str]
    upstream: List[str]
    downstream: List[str]
    depth: int  # longest path from any root

    def __str__(self) -> str:
        lines = [f"Task: {self.task_id}"]
        if self.description:
            lines.append(f"  Description : {self.description}")
        if self.timeout is not None:
            lines.append(f"  Timeout     : {self.timeout}s")
        if self.tags:
            lines.append(f"  Tags        : {', '.join(sorted(self.tags))}")
        lines.append(f"  Upstream    : {', '.join(self.upstream) or '(none)'}")
        lines.append(f"  Downstream  : {', '.join(self.downstream) or '(none)'}")
        lines.append(f"  Depth       : {self.depth}")
        return "\n".join(lines)


@dataclass
class InspectionReport:
    """Aggregated inspection results for a whole DAG."""

    dag_name: str
    tasks: Dict[str, TaskInspection] = field(default_factory=dict)

    def get(self, task_id: str) -> Optional[TaskInspection]:
        return self.tasks.get(task_id)

    def __len__(self) -> int:
        return len(self.tasks)


class DAGInspector:
    """Inspect tasks within a DAG and compute dependency metadata."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def inspect(self) -> InspectionReport:
        report = InspectionReport(dag_name=self._dag.name)
        depths = self._compute_depths()
        upstream_map = self._build_upstream_map()
        downstream_map = self._build_downstream_map()

        for task in self._dag.tasks.values():
            report.tasks[task.task_id] = TaskInspection(
                task_id=task.task_id,
                description=task.metadata.get("description"),
                timeout=task.metadata.get("timeout"),
                tags=list(task.metadata.get("tags", [])),
                upstream=sorted(upstream_map.get(task.task_id, [])),
                downstream=sorted(downstream_map.get(task.task_id, [])),
                depth=depths.get(task.task_id, 0),
            )
        return report

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _build_upstream_map(self) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {t: [] for t in self._dag.tasks}
        for task in self._dag.tasks.values():
            for dep in task.dependencies:
                result.setdefault(task.task_id, []).append(dep)
        return result

    def _build_downstream_map(self) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {t: [] for t in self._dag.tasks}
        for task in self._dag.tasks.values():
            for dep in task.dependencies:
                result.setdefault(dep, []).append(task.task_id)
        return result

    def _compute_depths(self) -> Dict[str, int]:
        depths: Dict[str, int] = {}

        def dfs(task_id: str) -> int:
            if task_id in depths:
                return depths[task_id]
            task = self._dag.tasks[task_id]
            if not task.dependencies:
                depths[task_id] = 0
            else:
                depths[task_id] = 1 + max(dfs(d) for d in task.dependencies)
            return depths[task_id]

        for tid in self._dag.tasks:
            dfs(tid)
        return depths
