"""Integration tests for the optimizer working with real DAG structures."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.optimizer import DAGOptimizer


def _chain(n: int) -> DAG:
    """Build a simple linear chain of n tasks."""
    dag = DAG(name="chain")
    for i in range(n):
        deps = [f"t{i - 1}"] if i > 0 else []
        dag.add_task(Task(task_id=f"t{i}", dependencies=deps))
    return dag


def _diamond() -> DAG:
    """A -> B, A -> C, B -> D, C -> D."""
    dag = DAG(name="diamond")
    dag.add_task(Task(task_id="a"))
    dag.add_task(Task(task_id="b", dependencies=["a"]))
    dag.add_task(Task(task_id="c", dependencies=["a"]))
    dag.add_task(Task(task_id="d", dependencies=["b", "c"]))
    return dag


def _redundant_diamond() -> DAG:
    """Diamond PLUS direct a->d edge (redundant)."""
    dag = DAG(name="redundant_diamond")
    dag.add_task(Task(task_id="a"))
    dag.add_task(Task(task_id="b", dependencies=["a"]))
    dag.add_task(Task(task_id="c", dependencies=["a"]))
    dag.add_task(Task(task_id="d", dependencies=["a", "b", "c"]))
    return dag


class TestOptimizerIntegration:
    def test_single_task_dag_no_hints(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only"))
        result = DAGOptimizer().optimize(dag)
        assert not result.has_hints

    def test_chain_of_two_no_redundant_edge(self):
        dag = DAG(name="two")
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        result = DAGOptimizer().optimize(dag)
        redundant = [h for h in result.hints if h.kind == "redundant_edge"]
        assert len(redundant) == 0

    def test_long_chain_parallelism_hint(self):
        dag = _chain(5)
        result = DAGOptimizer().optimize(dag)
        kinds = [h.kind for h in result.hints]
        assert "parallelism" in kinds

    def test_diamond_no_redundant_edges(self):
        result = DAGOptimizer().optimize(_diamond())
        redundant = [h for h in result.hints if h.kind == "redundant_edge"]
        assert len(redundant) == 0

    def test_redundant_diamond_flags_edge(self):
        result = DAGOptimizer().optimize(_redundant_diamond())
        redundant = [h for h in result.hints if h.kind == "redundant_edge"]
        assert len(redundant) >= 1

    def test_redundant_diamond_description_mentions_task(self):
        result = DAGOptimizer().optimize(_redundant_diamond())
        redundant = [h for h in result.hints if h.kind == "redundant_edge"]
        descriptions = " ".join(h.description for h in redundant)
        assert "a" in descriptions

    def test_wide_dag_no_parallelism_hint(self):
        """Many independent root tasks should not trigger parallelism hint."""
        dag = DAG(name="wide")
        for i in range(4):
            dag.add_task(Task(task_id=f"root_{i}"))
        dag.add_task(Task(task_id="sink", dependencies=[f"root_{i}" for i in range(4)]))
        result = DAGOptimizer().optimize(dag)
        kinds = [h.kind for h in result.hints]
        assert "parallelism" not in kinds
