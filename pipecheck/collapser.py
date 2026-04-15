"""Collapser: merge linear chains of tasks into single collapsed nodes."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipecheck.dag import DAG, Task


class CollapseError(Exception):
    """Raised when a collapse operation cannot be completed."""


@dataclass
class CollapseEntry:
    """Records one collapsed chain."""
    original_ids: List[str]
    collapsed_id: str

    def __str__(self) -> str:
        chain = " -> ".join(self.original_ids)
        return f"{chain}  =>  {self.collapsed_id}"


@dataclass
class CollapseResult:
    """Result of a collapse operation."""
    dag_name: str
    entries: List[CollapseEntry] = field(default_factory=list)
    dag: DAG = field(default=None)

    @property
    def has_collapses(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.entries:
            return f"CollapseResult({self.dag_name}): no chains collapsed"
        lines = [f"CollapseResult({self.dag_name}): {self.count} chain(s) collapsed"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGCollapser:
    """Collapses linear chains (A->B->C where B has degree 1 in/out) into one node."""

    def collapse(self, dag: DAG) -> CollapseResult:
        result = CollapseResult(dag_name=dag.name)
        visited: set = set()
        chains: List[List[str]] = []

        in_degree = {t: 0 for t in dag.tasks}
        out_degree = {t: 0 for t in dag.tasks}
        for src, dst in dag.edges:
            out_degree[src] = out_degree.get(src, 0) + 1
            in_degree[dst] = in_degree.get(dst, 0) + 1

        def _chain_start(tid: str) -> bool:
            return in_degree.get(tid, 0) != 1 or out_degree.get(tid, 0) != 1

        for task_id in list(dag.tasks.keys()):
            if task_id in visited:
                continue
            if out_degree.get(task_id, 0) == 1 and in_degree.get(task_id, 0) != 1:
                chain = [task_id]
                visited.add(task_id)
                nxt = next((d for s, d in dag.edges if s == task_id), None)
                while nxt and in_degree.get(nxt, 0) == 1 and out_degree.get(nxt, 0) == 1:
                    chain.append(nxt)
                    visited.add(nxt)
                    nxt = next((d for s, d in dag.edges if s == nxt), None)
                if nxt and nxt not in visited:
                    chain.append(nxt)
                    visited.add(nxt)
                if len(chain) > 1:
                    chains.append(chain)

        new_dag = DAG(name=dag.name)
        collapsed_ids: dict = {}
        for chain in chains:
            cid = "__".join(chain)
            for tid in chain:
                collapsed_ids[tid] = cid

        added: set = set()
        for tid, task in dag.tasks.items():
            rep = collapsed_ids.get(tid, tid)
            if rep not in added:
                new_dag.add_task(Task(id=rep, description=f"collapsed: {rep}"))
                added.add(rep)

        for src, dst in dag.edges:
            rsrc = collapsed_ids.get(src, src)
            rdst = collapsed_ids.get(dst, dst)
            if rsrc != rdst and (rsrc, rdst) not in new_dag.edges:
                new_dag.add_edge(rsrc, rdst)

        for chain in chains:
            entry = CollapseEntry(original_ids=chain, collapsed_id="__".join(chain))
            result.entries.append(entry)

        result.dag = new_dag
        return result
