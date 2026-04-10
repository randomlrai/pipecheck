"""DAG version pinning: record and compare pinned task configurations."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class PinError(Exception):
    """Raised when a pinning operation fails."""


@dataclass
class PinnedTask:
    """Immutable snapshot of a single task's key attributes."""

    task_id: str
    timeout: Optional[int]
    retries: int
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "timeout": self.timeout,
            "retries": self.retries,
            "tags": sorted(self.tags),
        }

    def __str__(self) -> str:
        return (
            f"PinnedTask({self.task_id!r}, timeout={self.timeout}, "
            f"retries={self.retries})"
        )


@dataclass
class DAGPin:
    """A pinned version of an entire DAG."""

    dag_name: str
    tasks: Dict[str, PinnedTask] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "dag_name": self.dag_name,
            "tasks": {tid: t.to_dict() for tid, t in self.tasks.items()},
        }

    def task_ids(self) -> List[str]:
        return sorted(self.tasks.keys())


class DAGPinner:
    """Creates and compares DAG pins."""

    def pin(self, dag: DAG) -> DAGPin:
        """Capture the current state of *dag* as a :class:`DAGPin`."""
        pinned = DAGPin(dag_name=dag.name)
        for task in dag.tasks.values():
            meta = task.metadata or {}
            pinned.tasks[task.task_id] = PinnedTask(
                task_id=task.task_id,
                timeout=meta.get("timeout"),
                retries=int(meta.get("retries", 0)),
                tags=list(meta.get("tags", [])),
            )
        return pinned

    def diff_pins(self, old: DAGPin, new: DAGPin) -> List[str]:
        """Return human-readable change lines between two pins."""
        changes: List[str] = []
        old_ids = set(old.tasks)
        new_ids = set(new.tasks)

        for tid in sorted(old_ids - new_ids):
            changes.append(f"removed task: {tid}")
        for tid in sorted(new_ids - old_ids):
            changes.append(f"added task: {tid}")
        for tid in sorted(old_ids & new_ids):
            o, n = old.tasks[tid], new.tasks[tid]
            if o.timeout != n.timeout:
                changes.append(
                    f"{tid}: timeout {o.timeout} -> {n.timeout}"
                )
            if o.retries != n.retries:
                changes.append(
                    f"{tid}: retries {o.retries} -> {n.retries}"
                )
            if sorted(o.tags) != sorted(n.tags):
                changes.append(
                    f"{tid}: tags {sorted(o.tags)} -> {sorted(n.tags)}"
                )
        return changes
