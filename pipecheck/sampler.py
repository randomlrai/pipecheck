"""DAG task sampler — select a representative subset of tasks from a DAG."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.dag import DAG, Task


class SampleError(Exception):
    """Raised when sampling cannot be performed."""


@dataclass
class SampleResult:
    dag_name: str
    sampled: List[Task] = field(default_factory=list)
    total: int = 0
    seed: Optional[int] = None

    @property
    def count(self) -> int:
        return len(self.sampled)

    @property
    def has_sample(self) -> bool:
        return len(self.sampled) > 0

    def __str__(self) -> str:
        lines = [f"Sample of '{self.dag_name}': {self.count}/{self.total} tasks"]
        if self.seed is not None:
            lines.append(f"  seed: {self.seed}")
        for task in self.sampled:
            lines.append(f"  - {task.task_id}")
        return "\n".join(lines)


class DAGSampler:
    """Randomly or deterministically sample tasks from a DAG."""

    def sample(
        self,
        dag: DAG,
        n: int,
        seed: Optional[int] = None,
    ) -> SampleResult:
        tasks = list(dag.tasks.values())
        total = len(tasks)

        if n < 0:
            raise SampleError("Sample size must be non-negative.")
        if n > total:
            raise SampleError(
                f"Requested {n} tasks but DAG only has {total}."
            )

        rng = random.Random(seed)
        chosen = rng.sample(tasks, n)

        return SampleResult(
            dag_name=dag.name,
            sampled=chosen,
            total=total,
            seed=seed,
        )
