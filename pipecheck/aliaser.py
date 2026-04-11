"""DAG task aliasing — map canonical task IDs to human-friendly aliases."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG


class AliasError(Exception):
    """Raised when an alias operation fails."""


@dataclass
class AliasEntry:
    task_id: str
    alias: str

    def __str__(self) -> str:
        return f"{self.task_id} -> {self.alias}"


@dataclass
class AliasResult:
    dag_name: str
    entries: List[AliasEntry] = field(default_factory=list)
    unresolved: List[str] = field(default_factory=list)

    @property
    def has_aliases(self) -> bool:
        return len(self.entries) > 0

    @property
    def has_unresolved(self) -> bool:
        return len(self.unresolved) > 0

    def __str__(self) -> str:
        lines = [f"Aliases for DAG '{self.dag_name}':"]
        if self.entries:
            for e in self.entries:
                lines.append(f"  {e}")
        else:
            lines.append("  (none)")
        if self.unresolved:
            lines.append("Unresolved task IDs:")
            for u in self.unresolved:
                lines.append(f"  {u}")
        return "\n".join(lines)


class DAGAliaser:
    """Apply a mapping of task_id -> alias to a DAG."""

    def alias(self, dag: DAG, mapping: Dict[str, str]) -> AliasResult:
        """Return an AliasResult describing resolved and unresolved aliases."""
        task_ids = {t.task_id for t in dag.tasks}
        entries: List[AliasEntry] = []
        unresolved: List[str] = []

        for task_id, alias in mapping.items():
            if not alias or not alias.strip():
                raise AliasError(f"Alias for '{task_id}' must be a non-empty string.")
            if task_id in task_ids:
                entries.append(AliasEntry(task_id=task_id, alias=alias.strip()))
            else:
                unresolved.append(task_id)

        entries.sort(key=lambda e: e.task_id)
        unresolved.sort()
        return AliasResult(dag_name=dag.name, entries=entries, unresolved=unresolved)

    def lookup(self, result: AliasResult, task_id: str) -> Optional[str]:
        """Return the alias for a task_id, or None if not aliased."""
        for entry in result.entries:
            if entry.task_id == task_id:
                return entry.alias
        return None
