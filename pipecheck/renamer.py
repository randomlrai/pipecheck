"""DAG task renaming with conflict detection and rollback support."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipecheck.dag import DAG, Task


class RenameError(Exception):
    """Raised when a rename operation cannot be completed."""


@dataclass
class RenameEntry:
    old_id: str
    new_id: str

    def __str__(self) -> str:
        return f"{self.old_id} -> {self.new_id}"


@dataclass
class RenameResult:
    dag: DAG
    renames: List[RenameEntry] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [f"RenameResult for '{self.dag.name}'"]
        for entry in self.renames:
            lines.append(f"  renamed: {entry}")
        for s in self.skipped:
            lines.append(f"  skipped: {s}")
        return "\n".join(lines)

    @property
    def has_renames(self) -> bool:
        return len(self.renames) > 0


class DAGRenamer:
    """Applies a rename map to task IDs within a DAG, updating edges."""

    def rename(self, dag: DAG, rename_map: Dict[str, str]) -> RenameResult:
        """Return a new DAG with tasks renamed according to *rename_map*."""
        self._validate_map(dag, rename_map)
        new_dag = DAG(name=dag.name)
        id_map: Dict[str, str] = {}
        renames: List[RenameEntry] = []
        skipped: List[str] = []

        for task in dag.tasks.values():
            new_id = rename_map.get(task.task_id, task.task_id)
            if new_id != task.task_id:
                renames.append(RenameEntry(old_id=task.task_id, new_id=new_id))
            else:
                skipped.append(task.task_id)
            id_map[task.task_id] = new_id
            new_task = Task(
                task_id=new_id,
                name=task.name,
                depends_on=[],
                metadata=dict(task.metadata),
            )
            new_dag.add_task(new_task)

        for task in dag.tasks.values():
            new_id = id_map[task.task_id]
            for dep in task.depends_on:
                new_dep = id_map[dep]
                new_dag.tasks[new_id].depends_on.append(new_dep)
                new_dag.add_edge(new_dep, new_id)

        return RenameResult(dag=new_dag, renames=renames, skipped=skipped)

    def _validate_map(self, dag: DAG, rename_map: Dict[str, str]) -> None:
        existing_ids = set(dag.tasks.keys())
        for old_id in rename_map:
            if old_id not in existing_ids:
                raise RenameError(f"Task '{old_id}' not found in DAG '{dag.name}'")
        new_ids = list(rename_map.values())
        if len(new_ids) != len(set(new_ids)):
            raise RenameError("Rename map contains duplicate target IDs")
        for new_id in new_ids:
            if new_id in existing_ids and new_id not in rename_map:
                raise RenameError(
                    f"Target ID '{new_id}' already exists and is not being renamed"
                )
