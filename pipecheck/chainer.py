"""Chain analysis for DAG pipelines."""
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG


class ChainError(Exception):
    """Raised when chain analysis fails."""


@dataclass
class Chain:
    """A linear sequence of tasks with no branching."""

    task_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        if not self.task_ids:
            return "Chain(empty)"
        return " -> ".join(self.task_ids)

    def __len__(self) -> int:
        return len(self.task_ids)


@dataclass
class ChainResult:
    """Result of chain extraction from a DAG."""

    dag_name: str
    chains: List[Chain] = field(default_factory=list)

    def longest(self) -> Optional[Chain]:
        """Return the longest chain, or None if no chains exist."""
        if not self.chains:
            return None
        return max(self.chains, key=len)

    def count(self) -> int:
        return len(self.chains)

    def __str__(self) -> str:
        lines = [f"ChainResult(dag={self.dag_name}, chains={self.count()})"]
        for i, chain in enumerate(self.chains, 1):
            lines.append(f"  [{i}] {chain}")
        return "\n".join(lines)


class DAGChainer:
    """Extracts maximal linear chains from a DAG."""

    def __init__(self, dag: DAG) -> None:
        self._dag = dag

    def extract(self) -> ChainResult:
        """Find all maximal linear chains (no branching/merging)."""
        dag = self._dag
        visited: set = set()
        chains: List[Chain] = []

        # Build successor/predecessor count maps
        successors = {t.task_id: [] for t in dag.tasks}
        predecessors = {t.task_id: [] for t in dag.tasks}
        for src, dst in dag.edges:
            successors[src].append(dst)
            predecessors[dst].append(src)

        def is_chain_start(tid: str) -> bool:
            # A chain starts where there are no predecessors OR
            # the predecessor has multiple successors
            preds = predecessors[tid]
            if not preds:
                return True
            if len(preds) > 1:
                return True
            return len(successors[preds[0]]) > 1

        for task in dag.tasks:
            tid = task.task_id
            if tid in visited or not is_chain_start(tid):
                continue
            chain_ids = [tid]
            visited.add(tid)
            current = tid
            while len(successors[current]) == 1:
                nxt = successors[current][0]
                if len(predecessors[nxt]) != 1 or nxt in visited:
                    break
                chain_ids.append(nxt)
                visited.add(nxt)
                current = nxt
            if len(chain_ids) >= 2:
                chains.append(Chain(task_ids=chain_ids))

        return ChainResult(dag_name=dag.name, chains=chains)
