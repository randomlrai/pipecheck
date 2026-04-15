"""Rewirer: redirect edges between tasks in a DAG."""
from dataclasses import dataclass, field
from typing import List
from pipecheck.dag import DAG, Task


class RewireError(Exception):
    """Raised when a rewire operation cannot be completed."""


@dataclass
class RewireEntry:
    """Records a single edge change."""
    old_source: str
    old_target: str
    new_source: str
    new_target: str

    def __str__(self) -> str:
        return (
            f"{self.old_source} -> {self.old_target}  "
            f"rewired to  {self.new_source} -> {self.new_target}"
        )


@dataclass
class RewireResult:
    """Holds the rewritten DAG and a log of every change made."""
    dag: DAG
    entries: List[RewireEntry] = field(default_factory=list)

    def has_changes(self) -> bool:
        return bool(self.entries)

    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.entries:
            return f"RewireResult({self.dag.name}): no changes"
        lines = [f"RewireResult({self.dag.name}): {self.count()} rewire(s)"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGRewirer:
    """Redirect one or more edges in a DAG, producing a new DAG."""

    def rewire(
        self,
        dag: DAG,
        old_source: str,
        old_target: str,
        new_source: str,
        new_target: str,
    ) -> RewireResult:
        """Replace the edge (old_source -> old_target) with (new_source -> new_target)."""
        task_ids = {t.task_id for t in dag.tasks}

        if old_source not in task_ids or old_target not in task_ids:
            raise RewireError(
                f"Edge {old_source} -> {old_target} not found in DAG '{dag.name}'."
            )
        if (old_target not in dag.dependencies.get(old_source, []) and
                old_source not in dag.dependencies.get(old_target, [])):
            # check adjacency list stored as dependents
            found = False
            for src, deps in dag.dependencies.items():
                if src == old_source and old_target in deps:
                    found = True
                    break
            if not found:
                raise RewireError(
                    f"Edge {old_source} -> {old_target} does not exist in DAG '{dag.name}'."
                )
        if new_source not in task_ids:
            raise RewireError(f"Task '{new_source}' not found in DAG '{dag.name}'.")
        if new_target not in task_ids:
            raise RewireError(f"Task '{new_target}' not found in DAG '{dag.name}'.")

        new_dag = DAG(name=dag.name)
        for task in dag.tasks:
            new_dag.add_task(task)

        for src, deps in dag.dependencies.items():
            for tgt in deps:
                if src == old_source and tgt == old_target:
                    new_dag.add_dependency(new_source, new_target)
                else:
                    new_dag.add_dependency(src, tgt)

        entry = RewireEntry(
            old_source=old_source,
            old_target=old_target,
            new_source=new_source,
            new_target=new_target,
        )
        return RewireResult(dag=new_dag, entries=[entry])
