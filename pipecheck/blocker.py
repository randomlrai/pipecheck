"""DAG task blocker — marks tasks as blocked and reports blocking chains."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.dag import DAG


class BlockError(Exception):
    """Raised when a blocking operation fails."""


@dataclass
class BlockEntry:
    task_id: str
    reason: Optional[str] = None
    blocked_by: Optional[str] = None  # upstream task_id that caused cascading block

    def __str__(self) -> str:
        parts = [f"BLOCKED: {self.task_id}"]
        if self.blocked_by:
            parts.append(f"(cascades from {self.blocked_by})")
        if self.reason:
            parts.append(f"- {self.reason}")
        return " ".join(parts)


@dataclass
class BlockResult:
    dag_name: str
    entries: List[BlockEntry] = field(default_factory=list)

    def has_blocks(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def cascaded_count(self) -> int:
        """Return the number of entries that were added via cascade (not directly blocked)."""
        return sum(1 for e in self.entries if e.blocked_by is not None)

    def __str__(self) -> str:
        if not self.entries:
            return f"[{self.dag_name}] No blocked tasks."
        lines = [f"[{self.dag_name}] Blocked tasks ({self.count}):"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGBlocker:
    """Marks tasks as blocked and optionally cascades to downstream dependents."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def block(
        self,
        task_ids: List[str],
        reason: Optional[str] = None,
        cascade: bool = False,
    ) -> BlockResult:
        all_task_ids = {t.task_id for t in self._dag.tasks}
        result = BlockResult(dag_name=self._dag.name)

        for tid in task_ids:
            if tid not in all_task_ids:
                raise BlockError(f"Task '{tid}' not found in DAG '{self._dag.name}'.")
            result.entries.append(BlockEntry(task_id=tid, reason=reason))

        if cascade:
            blocked_set = set(task_ids)
            for entry in list(result.entries):
                downstream = self._descendants(entry.task_id)
                for dtid in downstream:
                    if dtid not in blocked_set:
                        blocked_set.add(dtid)
                        result.entries.append(
                            BlockEntry(task_id=dtid, reason=reason, blocked_by=entry.task_id)
                        )

        return result

    def _descendants(self, task_id: str) -> List[str]:
        """Return all transitive downstream task IDs via BFS."""
        visited: List[str] = []
        queue = [task_id]
        seen = {task_id}
        while queue:
            current = queue.pop(0)
            for dep_id, upstream_id in self._dag.edges:
                if upstream_id == current and dep_id not in seen:
                    seen.add(dep_id)
                    visited.append(dep_id)
                    queue.append(dep_id)
        return visited
