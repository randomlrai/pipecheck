"""DAG snapshot utilities for saving and loading DAG state to disk."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

from pipecheck.dag import DAG, Task


class SnapshotError(Exception):
    """Raised when a snapshot operation fails."""


class DAGSnapshot:
    """Captures and restores the state of a DAG at a point in time."""

    VERSION = "1"

    def __init__(self, dag: DAG, label: Optional[str] = None) -> None:
        self.dag = dag
        self.label = label or dag.name
        self.created_at: str = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        tasks = [
            {
                "id": t.task_id,
                "description": t.description,
                "timeout": t.timeout,
                "metadata": t.metadata,
                "dependencies": list(t.dependencies),
            }
            for t in self.dag.tasks.values()
        ]
        return {
            "version": self.VERSION,
            "label": self.label,
            "created_at": self.created_at,
            "dag": {"name": self.dag.name, "tasks": tasks},
        }

    def save(self, path: str) -> None:
        """Persist the snapshot as JSON to *path*."""
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(self.to_dict(), fh, indent=2)
        except OSError as exc:
            raise SnapshotError(f"Cannot write snapshot to '{path}': {exc}") from exc

    # ------------------------------------------------------------------
    # Deserialisation
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, path: str) -> "DAGSnapshot":
        """Load a snapshot from *path* and return a :class:`DAGSnapshot`."""
        if not os.path.exists(path):
            raise SnapshotError(f"Snapshot file not found: '{path}'")
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise SnapshotError(f"Cannot read snapshot '{path}': {exc}") from exc

        dag_data = data.get("dag", {})
        dag = DAG(name=dag_data.get("name", "unnamed"))
        for t in dag_data.get("tasks", []):
            task = Task(
                task_id=t["id"],
                description=t.get("description"),
                timeout=t.get("timeout"),
                dependencies=set(t.get("dependencies", [])),
                metadata=t.get("metadata", {}),
            )
            dag.add_task(task)

        snap = cls(dag, label=data.get("label"))
        snap.created_at = data.get("created_at", snap.created_at)
        return snap
