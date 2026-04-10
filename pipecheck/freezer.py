"""DAG freezer: mark a DAG as frozen to prevent further modifications."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from pipecheck.dag import DAG


class FreezeError(Exception):
    """Raised when an operation is attempted on a frozen DAG."""


@dataclass
class FrozenDAG:
    """Represents a DAG that has been frozen at a specific point in time."""

    dag: DAG
    frozen_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reason: Optional[str] = None
    frozen_by: Optional[str] = None

    def __str__(self) -> str:
        parts = [f"FrozenDAG(dag={self.dag.name!r}, frozen_at={self.frozen_at.isoformat()}"]
        if self.frozen_by:
            parts.append(f", frozen_by={self.frozen_by!r}")
        if self.reason:
            parts.append(f", reason={self.reason!r}")
        parts.append(")")
        return "".join(parts)

    def to_dict(self) -> dict:
        return {
            "dag_name": self.dag.name,
            "task_count": len(self.dag.tasks),
            "frozen_at": self.frozen_at.isoformat(),
            "frozen_by": self.frozen_by,
            "reason": self.reason,
        }


class DAGFreezer:
    """Manages freezing and unfreezing of DAGs."""

    def __init__(self) -> None:
        self._frozen: dict[str, FrozenDAG] = {}

    def freeze(
        self,
        dag: DAG,
        reason: Optional[str] = None,
        frozen_by: Optional[str] = None,
    ) -> FrozenDAG:
        """Freeze a DAG, preventing it from being modified."""
        if dag.name in self._frozen:
            raise FreezeError(f"DAG '{dag.name}' is already frozen.")
        frozen = FrozenDAG(dag=dag, reason=reason, frozen_by=frozen_by)
        self._frozen[dag.name] = frozen
        return frozen

    def unfreeze(self, dag_name: str) -> None:
        """Unfreeze a previously frozen DAG."""
        if dag_name not in self._frozen:
            raise FreezeError(f"DAG '{dag_name}' is not frozen.")
        del self._frozen[dag_name]

    def is_frozen(self, dag_name: str) -> bool:
        """Return True if the DAG with the given name is currently frozen."""
        return dag_name in self._frozen

    def get(self, dag_name: str) -> Optional[FrozenDAG]:
        """Return the FrozenDAG record for a given DAG name, or None."""
        return self._frozen.get(dag_name)

    def all_frozen(self) -> list[FrozenDAG]:
        """Return all currently frozen DAGs, sorted by name."""
        return [self._frozen[k] for k in sorted(self._frozen)]
