"""Tests for pipecheck.optimizer."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.optimizer import DAGOptimizer, OptimizeHint, OptimizeResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    return dag


# ---------------------------------------------------------------------------
class TestOptimizeHint:
    def test_str_format(self):
        h = OptimizeHint(kind="redundant_edge", description="A -> B is redundant")
        assert str(h) == "[redundant_edge] A -> B is redundant"


class TestOptimizeResult:
    def test_has_hints_false_when_empty(self):
        r = OptimizeResult(dag_name="pipe")
        assert not r.has_hints

    def test_has_hints_true_when_populated(self):
        r = OptimizeResult(dag_name="pipe", hints=[OptimizeHint("parallelism", "x")])
        assert r.has_hints

    def test_count_property(self):
        r = OptimizeResult(dag_name="pipe", hints=[OptimizeHint("parallelism", "x")])
        assert r.count == 1

    def test_str_no_hints(self):
        r = OptimizeResult(dag_name="mypipe")
        assert "no optimizations" in str(r)
        assert "mypipe" in str(r)

    def test_str_with_hints(self):
        r = OptimizeResult(
            dag_name="mypipe",
            hints=[OptimizeHint("redundant_edge", "A -> C is implied")],
        )
        text = str(r)
        assert "1 optimization hint" in text
        assert "redundant_edge" in text


class TestDAGOptimizer:
    def test_empty_dag_no_hints(self):
        dag = _build_dag()
        result = DAGOptimizer().optimize(dag)
        assert not result.has_hints

    def test_linear_chain_single_root_parallelism_hint(self):
        dag = _build_dag()
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        dag.add_task(Task(task_id="c", dependencies=["b"]))
        result = DAGOptimizer().optimize(dag)
        kinds = [h.kind for h in result.hints]
        assert "parallelism" in kinds

    def test_multiple_roots_no_parallelism_hint(self):
        dag = _build_dag()
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b"))
        dag.add_task(Task(task_id="c", dependencies=["a", "b"]))
        result = DAGOptimizer().optimize(dag)
        kinds = [h.kind for h in result.hints]
        assert "parallelism" not in kinds

    def test_redundant_edge_detected(self):
        # a -> b -> c, AND a -> c directly (redundant)
        dag = _build_dag()
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        dag.add_task(Task(task_id="c", dependencies=["a", "b"]))
        result = DAGOptimizer().optimize(dag)
        kinds = [h.kind for h in result.hints]
        assert "redundant_edge" in kinds

    def test_no_redundant_edge_in_clean_dag(self):
        dag = _build_dag()
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        dag.add_task(Task(task_id="c", dependencies=["b"]))
        result = DAGOptimizer().optimize(dag)
        kinds = [h.kind for h in result.hints]
        assert "redundant_edge" not in kinds

    def test_result_dag_name_matches(self):
        dag = _build_dag(name="my_pipeline")
        result = DAGOptimizer().optimize(dag)
        assert result.dag_name == "my_pipeline"
