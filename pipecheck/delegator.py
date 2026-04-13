"""Delegator: assign tasks in a DAG to named owners or teams."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class DelegateError(Exception):
    """Raised when delegation cannot be performed."""


@dataclass
class DelegateEntry:
    task_id: str
    owner: str
    reason: Optional[str] = None

    def __str__(self) -> str:
        base = f"{self.task_id} -> {self.owner}"
        if self.reason:
            base += f" ({self.reason})"
        return base


@dataclass
class DelegateResult:
    dag_name: str
    entries: List[DelegateEntry] = field(default_factory=list)

    @property
    def has_assignments(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def owners(self) -> List[str]:
        """Return sorted unique list of owners."""
        return sorted({e.owner for e in self.entries})

    def tasks_for_owner(self, owner: str) -> List[str]:
        """Return task IDs assigned to a given owner."""
        return [e.task_id for e in self.entries if e.owner == owner]

    def __str__(self) -> str:
        if not self.entries:
            return f"DelegateResult({self.dag_name}): no assignments"
        lines = [f"DelegateResult({self.dag_name}):"] + [
            f"  {e}" for e in self.entries
        ]
        return "\n".join(lines)


class DAGDelegator:
    """Assign tasks to owners based on a mapping or tag rules."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def assign(
        self,
        assignments: Dict[str, str],
        reason: Optional[str] = None,
    ) -> DelegateResult:
        """Assign tasks to owners from a task_id -> owner mapping.

        Args:
            assignments: mapping of task_id to owner name.
            reason: optional reason applied to every entry.

        Returns:
            DelegateResult containing one entry per matched task.

        Raises:
            DelegateError: if any task_id is not present in the DAG.
        """
        task_ids = {t.task_id for t in self._dag.tasks}
        unknown = [tid for tid in assignments if tid not in task_ids]
        if unknown:
            raise DelegateError(
                f"Unknown task(s) in assignments: {', '.join(sorted(unknown))}"
            )

        result = DelegateResult(dag_name=self._dag.name)
        for task_id, owner in sorted(assignments.items()):
            result.entries.append(
                DelegateEntry(task_id=task_id, owner=owner, reason=reason)
            )
        return result
