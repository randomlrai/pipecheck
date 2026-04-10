"""DAG trimmer: removes unreachable or redundant tasks from a DAG."""

from dataclasses import dataclass, field
from typing import List, Set

from pipecheck.dag import DAG, Task


class TrimError(Exception):
    """Raised when trimming fails."""


@dataclass
class TrimResult:
    """Result of a DAG trim operation."""

    original_dag: DAG
    trimmed_dag: DAG
    removed_task_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        count = len(self.removed_task_ids)
        if count == 0:
            return f"DAG '{self.original_dag.name}': nothing to trim."
        ids = ", ".join(self.removed_task_ids)
        return (
            f"DAG '{self.original_dag.name}': removed {count} task(s): {ids}"
        )

    @property
    def has_removals(self) -> bool:
        return len(self.removed_task_ids) > 0


class DAGTrimmer:
    """Removes tasks that are unreachable from any root or are pure sinks
    with no downstream consumers when *sink_only* mode is active."""

    def trim_unreachable(self, dag: DAG) -> TrimResult:
        """Remove tasks not reachable from any root task (tasks with no deps)."""
        roots = self._find_roots(dag)
        if not roots:
            raise TrimError("DAG has no root tasks; cannot determine reachability.")

        reachable: Set[str] = set()
        queue = list(roots)
        while queue:
            tid = queue.pop()
            if tid in reachable:
                continue
            reachable.add(tid)
            task = dag.get_task(tid)
            for dep_id in task.dependencies:
                # traverse downstream: tasks that depend on this one
                pass
            # traverse downstream via edges
            for other in dag.tasks.values():
                if tid in other.dependencies and other.task_id not in reachable:
                    queue.append(other.task_id)

        removed = [tid for tid in dag.tasks if tid not in reachable]
        return self._build_result(dag, removed)

    def trim_isolated(self, dag: DAG) -> TrimResult:
        """Remove tasks that have no dependencies AND no dependents (isolated)."""
        dependents: Set[str] = set()
        for task in dag.tasks.values():
            dependents.update(task.dependencies)

        removed = [
            tid
            for tid, task in dag.tasks.items()
            if not task.dependencies and tid not in dependents and len(dag.tasks) > 1
        ]
        return self._build_result(dag, removed)

    # ------------------------------------------------------------------
    def _find_roots(self, dag: DAG) -> List[str]:
        return [tid for tid, t in dag.tasks.items() if not t.dependencies]

    def _build_result(self, dag: DAG, removed: List[str]) -> TrimResult:
        new_dag = DAG(name=dag.name)
        for tid, task in dag.tasks.items():
            if tid not in removed:
                new_deps = [d for d in task.dependencies if d not in removed]
                new_task = Task(
                    task_id=task.task_id,
                    description=task.description,
                    dependencies=new_deps,
                    metadata=dict(task.metadata),
                )
                new_dag.add_task(new_task)
        return TrimResult(
            original_dag=dag,
            trimmed_dag=new_dag,
            removed_task_ids=sorted(removed),
        )
