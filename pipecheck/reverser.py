"""Reverse the edges of a DAG to produce an inverted dependency graph."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG, Task


class ReverseError(Exception):
    """Raised when a DAG cannot be reversed."""


@dataclass
class ReverseResult:
    """Result of reversing a DAG."""

    original_name: str
    reversed_dag: DAG
    edge_count: int

    def __str__(self) -> str:  # noqa: D105
        return (
            f"ReverseResult(dag={self.original_name!r}, "
            f"tasks={len(self.reversed_dag.tasks)}, "
            f"edges={self.edge_count})"
        )

    @property
    def count(self) -> int:
        """Number of tasks in the reversed DAG."""
        return len(self.reversed_dag.tasks)


class DAGReverser:
    """Inverts all edges in a DAG so that each dependency points in reverse."""

    def reverse(self, dag: DAG) -> ReverseResult:
        """Return a new DAG with all edges flipped.

        Parameters
        ----------
        dag:
            The source DAG to reverse.

        Returns
        -------
        ReverseResult
            Contains the new reversed DAG and metadata.
        """
        if not dag.tasks:
            raise ReverseError(f"DAG '{dag.name}' has no tasks to reverse.")

        reversed_dag = DAG(name=f"{dag.name}_reversed")

        # Add all tasks (without dependencies first)
        for task in dag.tasks.values():
            reversed_dag.add_task(
                Task(
                    task_id=task.task_id,
                    description=task.description,
                    metadata=dict(task.metadata),
                )
            )

        # Flip each edge
        edge_count = 0
        for task in dag.tasks.values():
            for dep_id in task.dependencies:
                # original: dep_id -> task.task_id  becomes  task.task_id -> dep_id
                reversed_dag.tasks[dep_id].dependencies.append(task.task_id)
                edge_count += 1

        return ReverseResult(
            original_name=dag.name,
            reversed_dag=reversed_dag,
            edge_count=edge_count,
        )
