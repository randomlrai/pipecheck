"""DAG pruning: remove unreachable tasks from a given set of roots."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Set

from pipecheck.dag import DAG, Task


class PruneError(Exception):
    """Raised when pruning cannot be completed."""


@dataclass
class PruneResult:
    """Result of a pruning operation."""

    dag: DAG
    removed_tasks: List[str] = field(default_factory=list)
    kept_tasks: List[str] = field(default_factory=list)

    def __str__(self) -> str:  # noqa: D105
        removed = len(self.removed_tasks)
        kept = len(self.kept_tasks)
        lines = [f"PruneResult: kept={kept}, removed={removed}"]
        if self.removed_tasks:
            for tid in sorted(self.removed_tasks):
                lines.append(f"  - removed: {tid}")
        return "\n".join(lines)

    @property
    def has_removals(self) -> bool:
        """Return True when at least one task was pruned."""
        return bool(self.removed_tasks)


class DAGPruner:
    """Prune a DAG by keeping only tasks reachable from *root_ids*."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def _reachable(self, root_ids: Set[str]) -> Set[str]:
        """BFS/DFS to collect all task IDs reachable from *root_ids*.

        Raises
        ------
        PruneError
            If any task ID in *root_ids* does not exist in the DAG, or if a
            dependency referenced by a reachable task is missing.
        """
        visited: Set[str] = set()
        stack = list(root_ids)
        while stack:
            tid = stack.pop()
            if tid in visited:
                continue
            visited.add(tid)
            task = self._dag.get_task(tid)
            if task is None:
                raise PruneError(f"Task '{tid}' not found in DAG.")
            for dep in task.dependencies:
                if dep not in visited:
                    stack.append(dep)
        return visited

    def prune(self, root_ids: List[str]) -> PruneResult:
        """Return a new DAG containing only tasks reachable from *root_ids*.

        Parameters
        ----------
        root_ids:
            Task IDs that serve as the entry points of the pruned DAG.  All
            tasks that these roots depend on (transitively) are also kept.

        Raises
        ------
        PruneError
            If *root_ids* is empty or contains an unknown task ID.
        """
        if not root_ids:
            raise PruneError("At least one root task ID must be supplied.")

        # Validate that every requested root exists before doing any work.
        unknown = [tid for tid in root_ids if self._dag.get_task(tid) is None]
        if unknown:
            raise PruneError(
                f"Root task(s) not found in DAG: {', '.join(sorted(unknown))}"
            )

        reachable = self._reachable(set(root_ids))
        all_ids = {t.task_id for t in self._dag.tasks}
        removed = sorted(all_ids - reachable)
        kept = sorted(reachable)

        new_dag = DAG(name=self._dag.name)
        for task in self._dag.tasks:
            if task.task_id in reachable:
                pruned_deps = [d for d in task.dependencies if d in reachable]
                new_task = Task(
                    task_id=task.task_id,
                    name=task.name,
                    dependencies=pruned_deps,
                    metadata=dict(task.metadata),
                )
                new_dag.add_task(new_task)

        return PruneResult(dag=new_dag, removed_tasks=removed, kept_tasks=kept)
