"""Pinpointer: locate tasks in a DAG by position (root, leaf, mid-chain)."""

from dataclasses import dataclass, field
from typing import List

from pipecheck.dag import DAG


class PinpointError(Exception):
    """Raised when pinpointing fails."""


@dataclass
class PinpointResult:
    dag_name: str
    roots: List[str] = field(default_factory=list)
    leaves: List[str] = field(default_factory=list)
    intermediates: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [f"Pinpoint results for DAG '{self.dag_name}':"]
        lines.append(f"  Roots       : {', '.join(sorted(self.roots)) or 'none'}")
        lines.append(f"  Leaves      : {', '.join(sorted(self.leaves)) or 'none'}")
        lines.append(f"  Intermediates: {', '.join(sorted(self.intermediates)) or 'none'}")
        return "\n".join(lines)

    @property
    def count(self) -> int:
        return len(self.roots) + len(self.leaves) + len(self.intermediates)

    def has_intermediates(self) -> bool:
        return len(self.intermediates) > 0


class DAGPinpointer:
    """Classifies every task in a DAG as root, leaf, or intermediate."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def pinpoint(self) -> PinpointResult:
        dag = self._dag
        if not dag.tasks:
            return PinpointResult(dag_name=dag.name)

        all_ids = {t.task_id for t in dag.tasks.values()}

        has_incoming = set()
        has_outgoing = set()
        for src, dst in dag.edges:
            has_outgoing.add(src)
            has_incoming.add(dst)

        roots = sorted(all_ids - has_incoming)
        leaves = sorted(all_ids - has_outgoing)
        intermediates = sorted(has_incoming & has_outgoing)

        return PinpointResult(
            dag_name=dag.name,
            roots=roots,
            leaves=leaves,
            intermediates=intermediates,
        )
