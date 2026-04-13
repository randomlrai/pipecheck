"""DAG patching: apply field-level updates to tasks in a DAG."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pipecheck.dag import DAG, Task


class PatchError(Exception):
    """Raised when a patch operation fails."""


@dataclass
class PatchEntry:
    task_id: str
    field: str
    old_value: Any
    new_value: Any

    def __str__(self) -> str:
        return f"{self.task_id}.{self.field}: {self.old_value!r} -> {self.new_value!r}"


@dataclass
class PatchResult:
    dag_name: str
    patches: List[PatchEntry] = field(default_factory=list)

    @property
    def has_patches(self) -> bool:
        return len(self.patches) > 0

    @property
    def count(self) -> int:
        return len(self.patches)

    def __str__(self) -> str:
        if not self.patches:
            return f"PatchResult({self.dag_name}): no changes applied"
        lines = [f"PatchResult({self.dag_name}): {self.count} change(s)"]
        for p in self.patches:
            lines.append(f"  {p}")
        return "\n".join(lines)


class DAGPatcher:
    """Apply a dict of {task_id: {field: value}} patches to a DAG."""

    def patch(self, dag: DAG, spec: Dict[str, Dict[str, Any]]) -> PatchResult:
        result = PatchResult(dag_name=dag.name)
        for task_id, updates in spec.items():
            task = self._find_task(dag, task_id)
            if task is None:
                raise PatchError(f"Task '{task_id}' not found in DAG '{dag.name}'")
            for fld, new_val in updates.items():
                if not hasattr(task, fld):
                    raise PatchError(
                        f"Task '{task_id}' has no field '{fld}'"
                    )
                old_val = getattr(task, fld)
                setattr(task, fld, new_val)
                result.patches.append(
                    PatchEntry(task_id=task_id, field=fld,
                               old_value=old_val, new_value=new_val)
                )
        return result

    @staticmethod
    def _find_task(dag: DAG, task_id: str) -> Optional[Task]:
        for task in dag.tasks:
            if task.task_id == task_id:
                return task
        return None
