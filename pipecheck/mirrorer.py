"""DAG mirroring: produce a structurally identical copy with remapped task IDs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.dag import DAG, Task


class MirrorError(Exception):
    """Raised when mirroring fails."""


@dataclass
class MirrorEntry:
    original_id: str
    mirrored_id: str

    def __str__(self) -> str:
        return f"{self.original_id} -> {self.mirrored_id}"


@dataclass
class MirrorResult:
    dag_name: str
    dag: DAG
    entries: List[MirrorEntry] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.entries)

    @property
    def has_mappings(self) -> bool:
        return len(self.entries) > 0

    def mapping(self) -> Dict[str, str]:
        return {e.original_id: e.mirrored_id for e in self.entries}

    def __str__(self) -> str:
        lines = [f"Mirror of '{self.dag_name}' ({self.count} tasks)"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


class DAGMirrorer:
    """Creates a mirrored copy of a DAG with optionally prefixed task IDs."""

    def mirror(
        self,
        dag: DAG,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
    ) -> MirrorResult:
        if prefix is None and suffix is None:
            prefix = "mirror_"

        def remap(task_id: str) -> str:
            new_id = task_id
            if prefix:
                new_id = f"{prefix}{new_id}"
            if suffix:
                new_id = f"{new_id}{suffix}"
            return new_id

        id_map: Dict[str, str] = {t: remap(t) for t in dag.tasks}

        mirrored = DAG(name=f"{dag.name}_mirror")
        entries: List[MirrorEntry] = []

        for original_id, task in dag.tasks.items():
            new_id = id_map[original_id]
            new_task = Task(
                task_id=new_id,
                description=task.description,
                timeout=task.timeout,
                retries=task.retries,
                metadata=dict(task.metadata),
            )
            mirrored.add_task(new_task)
            entries.append(MirrorEntry(original_id=original_id, mirrored_id=new_id))

        for src, dst in dag.edges:
            mirrored.add_edge(id_map[src], id_map[dst])

        return MirrorResult(dag_name=dag.name, dag=mirrored, entries=entries)
