"""DAG merging utilities for combining multiple pipeline DAGs into one."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.dag import DAG, Task


class MergeError(Exception):
    """Raised when DAGs cannot be merged cleanly."""


@dataclass
class MergeConflict:
    """Describes a conflict encountered during a merge."""

    task_id: str
    reason: str

    def __str__(self) -> str:
        return f"Conflict on task '{self.task_id}': {self.reason}"


@dataclass
class MergeResult:
    """Result of merging two or more DAGs."""

    dag: DAG
    conflicts: List[MergeConflict] = field(default_factory=list)

    @property
    def has_conflicts(self) -> bool:
        return len(self.conflicts) > 0

    def __str__(self) -> str:
        lines = [f"MergeResult: DAG '{self.dag.name}' ({len(self.dag.tasks)} tasks)"]
        if self.conflicts:
            lines.append(f"  {len(self.conflicts)} conflict(s):")
            for c in self.conflicts:
                lines.append(f"    - {c}")
        else:
            lines.append("  No conflicts.")
        return "\n".join(lines)


class DAGMerger:
    """Merges multiple DAGs into a single combined DAG."""

    def __init__(self, merged_name: str = "merged") -> None:
        self.merged_name = merged_name

    def merge(self, *dags: DAG, on_conflict: str = "raise") -> MergeResult:
        """Merge one or more DAGs.

        Args:
            *dags: DAGs to merge.
            on_conflict: One of 'raise', 'skip', or 'overwrite'.
                - 'raise'     : raise MergeError on duplicate task IDs.
                - 'skip'      : keep the first definition, record a conflict.
                - 'overwrite' : use the last definition, record a conflict.

        Returns:
            MergeResult containing the merged DAG and any conflicts.
        """
        if on_conflict not in ("raise", "skip", "overwrite"):
            raise ValueError(f"Invalid on_conflict value: '{on_conflict}'")

        merged = DAG(name=self.merged_name)
        conflicts: List[MergeConflict] = []
        seen_ids: dict = {}

        for dag in dags:
            for task in dag.tasks.values():
                if task.task_id in seen_ids:
                    conflict = MergeConflict(
                        task_id=task.task_id,
                        reason=(
                            f"already defined in DAG '{seen_ids[task.task_id]}'; "
                            f"redefined in DAG '{dag.name}'"
                        ),
                    )
                    conflicts.append(conflict)
                    if on_conflict == "raise":
                        raise MergeError(str(conflict))
                    elif on_conflict == "skip":
                        continue
                    # overwrite: fall through to add_task

                merged.add_task(task)
                seen_ids[task.task_id] = dag.name

            for src_id, dst_id in dag.edges:
                if src_id in merged.tasks and dst_id in merged.tasks:
                    merged.add_edge(src_id, dst_id)

        return MergeResult(dag=merged, conflicts=conflicts)
