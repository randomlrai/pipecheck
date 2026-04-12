"""DAG task access control: enforce allow/deny rules on task execution."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.dag import DAG


class BounceError(Exception):
    """Raised when the bouncer encounters an invalid operation."""


@dataclass
class BounceEntry:
    task_id: str
    allowed: bool
    reason: Optional[str] = None

    def __str__(self) -> str:
        status = "ALLOW" if self.allowed else "DENY"
        base = f"[{status}] {self.task_id}"
        if self.reason:
            return f"{base} — {self.reason}"
        return base


@dataclass
class BounceResult:
    dag_name: str
    entries: List[BounceEntry] = field(default_factory=list)

    @property
    def has_denials(self) -> bool:
        return any(not e.allowed for e in self.entries)

    @property
    def denied(self) -> List[BounceEntry]:
        return [e for e in self.entries if not e.allowed]

    @property
    def allowed(self) -> List[BounceEntry]:
        return [e for e in self.entries if e.allowed]

    def __str__(self) -> str:
        lines = [f"BounceResult({self.dag_name})"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGBouncer:
    """Apply allow/deny rules to tasks in a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def evaluate(
        self,
        allow_tags: Optional[List[str]] = None,
        deny_tags: Optional[List[str]] = None,
        allow_prefixes: Optional[List[str]] = None,
        deny_prefixes: Optional[List[str]] = None,
    ) -> BounceResult:
        allow_tags = allow_tags or []
        deny_tags = deny_tags or []
        allow_prefixes = allow_prefixes or []
        deny_prefixes = deny_prefixes or []

        result = BounceResult(dag_name=self._dag.name)

        for task in self._dag.tasks.values():
            tid = task.task_id
            tags = set(task.metadata.get("tags", []))

            denied_by_tag = any(t in tags for t in deny_tags)
            denied_by_prefix = any(tid.startswith(p) for p in deny_prefixes)

            if denied_by_tag:
                result.entries.append(
                    BounceEntry(tid, allowed=False, reason=f"tag in deny list")
                )
                continue
            if denied_by_prefix:
                result.entries.append(
                    BounceEntry(tid, allowed=False, reason=f"prefix in deny list")
                )
                continue

            if allow_tags and not any(t in tags for t in allow_tags):
                result.entries.append(
                    BounceEntry(tid, allowed=False, reason="no matching allow tag")
                )
                continue
            if allow_prefixes and not any(tid.startswith(p) for p in allow_prefixes):
                result.entries.append(
                    BounceEntry(tid, allowed=False, reason="no matching allow prefix")
                )
                continue

            result.entries.append(BounceEntry(tid, allowed=True))

        return result
