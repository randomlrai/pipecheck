"""DAG digest / fingerprinting module.

Computes a stable hash fingerprint for a DAG so that two DAG definitions
can be compared for structural equality without a full diff.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipecheck.dag import DAG


class DigestError(Exception):
    """Raised when fingerprinting fails."""


@dataclass
class DigestResult:
    dag_name: str
    fingerprint: str
    task_count: int
    edge_count: int
    algorithm: str = "sha256"

    def __str__(self) -> str:
        lines = [
            f"DAG: {self.dag_name}",
            f"Algorithm : {self.algorithm}",
            f"Fingerprint: {self.fingerprint}",
            f"Tasks : {self.task_count}",
            f"Edges : {self.edge_count}",
        ]
        return "\n".join(lines)

    def short(self, length: int = 12) -> str:
        """Return a shortened fingerprint for display."""
        return self.fingerprint[:length]

    def matches(self, other: "DigestResult") -> bool:
        """Return True if both results share the same fingerprint."""
        return self.fingerprint == other.fingerprint


class DAGDigester:
    """Computes a deterministic fingerprint for a DAG."""

    def __init__(self, algorithm: str = "sha256") -> None:
        if algorithm not in hashlib.algorithms_available:
            raise DigestError(f"Unsupported hash algorithm: {algorithm!r}")
        self._algorithm = algorithm

    def digest(self, dag: "DAG") -> DigestResult:
        payload = self._build_payload(dag)
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        h = hashlib.new(self._algorithm, raw.encode())
        tasks = list(dag.tasks.values())
        edges: list[tuple[str, str]] = []
        for task in tasks:
            for dep in task.dependencies:
                edges.append((dep, task.task_id))
        return DigestResult(
            dag_name=dag.name,
            fingerprint=h.hexdigest(),
            task_count=len(tasks),
            edge_count=len(edges),
            algorithm=self._algorithm,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(self, dag: "DAG") -> dict:
        tasks_payload = []
        for task_id in sorted(dag.tasks):
            task = dag.tasks[task_id]
            tasks_payload.append({
                "id": task.task_id,
                "deps": sorted(task.dependencies),
                "timeout": task.timeout,
                "retries": task.retries,
            })
        return {"name": dag.name, "tasks": tasks_payload}
