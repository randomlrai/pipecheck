"""DAG cloning utilities for pipecheck."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipecheck.dag import DAG, Task


class CloneError(Exception):
    """Raised when a DAG cannot be cloned."""


@dataclass
class CloneResult:
    """Result of a DAG clone operation."""

    original_name: str
    cloned_dag: DAG
    task_count: int
    edge_count: int
    prefix: Optional[str] = None

    def __str__(self) -> str:
        prefix_info = f" (prefix='{self.prefix}')" if self.prefix else ""
        return (
            f"CloneResult: '{self.original_name}' -> '{self.cloned_dag.name}'"
            f"{prefix_info} "
            f"[{self.task_count} tasks, {self.edge_count} edges]"
        )


class DAGCloner:
    """Clones a DAG, optionally renaming tasks with a prefix."""

    def clone(
        self,
        dag: DAG,
        new_name: str,
        prefix: Optional[str] = None,
    ) -> CloneResult:
        """Return a deep copy of *dag* under *new_name*.

        Args:
            dag: The source DAG to clone.
            new_name: Name for the cloned DAG.
            prefix: Optional string prepended to every task id.

        Returns:
            A :class:`CloneResult` containing the new DAG and metadata.

        Raises:
            CloneError: If *dag* has no tasks.
        """
        if not dag.tasks:
            raise CloneError(f"Cannot clone empty DAG '{dag.name}'.")

        cloned = DAG(name=new_name)

        id_map: dict[str, str] = {}
        for task in dag.tasks.values():
            new_id = f"{prefix}{task.task_id}" if prefix else task.task_id
            id_map[task.task_id] = new_id
            cloned_task = Task(
                task_id=new_id,
                description=task.description,
                timeout=task.timeout,
                tags=list(task.tags),
                metadata=dict(task.metadata),
            )
            cloned.add_task(cloned_task)

        edge_count = 0
        for task in dag.tasks.values():
            for dep_id in task.dependencies:
                cloned.add_edge(id_map[dep_id], id_map[task.task_id])
                edge_count += 1

        return CloneResult(
            original_name=dag.name,
            cloned_dag=cloned,
            task_count=len(cloned.tasks),
            edge_count=edge_count,
            prefix=prefix,
        )
