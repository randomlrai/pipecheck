"""DAG reducer: collapse chains of single-dependency tasks into compound steps."""

from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.dag import DAG, Task


class ReduceError(Exception):
    """Raised when reduction cannot be performed."""


@dataclass
class ReduceEntry:
    """Represents one collapsed chain."""

    original_ids: List[str]
    compound_id: str

    def __str__(self) -> str:
        chain = " -> ".join(self.original_ids)
        return f"{chain} => {self.compound_id}"


@dataclass
class ReduceResult:
    """Holds the reduced DAG and a log of what was collapsed."""

    dag: DAG
    entries: List[ReduceEntry] = field(default_factory=list)

    @property
    def has_reductions(self) -> bool:
        return len(self.entries) > 0

    @property
    def count(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if not self.has_reductions:
            return f"ReduceResult(dag={self.dag.name}, no reductions)"
        lines = [f"ReduceResult(dag={self.dag.name}, {self.count} chain(s) collapsed):"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


class DAGReducer:
    """Collapses linear chains (A->B->C where B has one in and one out) into a
    single compound task whose id is the joined ids of the chain members."""

    JOIN = "__"

    def reduce(self, dag: DAG) -> ReduceResult:
        if not dag.tasks:
            raise ReduceError("Cannot reduce an empty DAG.")

        in_degree = {t: 0 for t in dag.tasks}
        out_degree = {t: 0 for t in dag.tasks}
        for src, dst in dag.edges:
            out_degree[src] += 1
            in_degree[dst] += 1

        visited: set = set()
        chains: List[List[str]] = []

        for task_id in dag.tasks:
            if task_id in visited:
                continue
            # Only start a chain from a node that is a chain member
            if in_degree[task_id] == 1 and out_degree.get(task_id, 0) <= 1:
                continue  # middle/end of chain — will be picked up by its head
            # Walk forward collecting chain
            chain = [task_id]
            visited.add(task_id)
            current = task_id
            while out_degree[current] == 1:
                successors = [d for s, d in dag.edges if s == current]
                nxt = successors[0]
                if in_degree[nxt] != 1 or nxt in visited:
                    break
                chain.append(nxt)
                visited.add(nxt)
                current = nxt
            if len(chain) > 1:
                chains.append(chain)

        new_dag = DAG(name=dag.name)
        id_map: dict = {}
        entries: List[ReduceEntry] = []

        for chain in chains:
            compound_id = self.JOIN.join(chain)
            head_task = dag.tasks[chain[0]]
            compound_task = Task(
                task_id=compound_id,
                description=head_task.description,
                timeout=max((dag.tasks[t].timeout or 0) for t in chain) or None,
                metadata={"reduced_from": chain},
            )
            new_dag.add_task(compound_task)
            for tid in chain:
                id_map[tid] = compound_id
            entries.append(ReduceEntry(original_ids=chain, compound_id=compound_id))

        for task_id, task in dag.tasks.items():
            if task_id not in id_map:
                new_dag.add_task(task)
                id_map[task_id] = task_id

        seen_edges: set = set()
        for src, dst in dag.edges:
            new_src = id_map[src]
            new_dst = id_map[dst]
            if new_src != new_dst:
                edge = (new_src, new_dst)
                if edge not in seen_edges:
                    new_dag.add_edge(new_src, new_dst)
                    seen_edges.add(edge)

        return ReduceResult(dag=new_dag, entries=entries)
