"""Tests for pipecheck.weigher."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.weigher import DAGWeigher, TaskWeight, WeighError, WeighResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    dag.add_task(Task(task_id="a", command="echo a", metadata={"weight": 2.0}))
    dag.add_task(Task(task_id="b", command="echo b", dependencies=["a"], metadata={"weight": 3.0}))
    dag.add_task(Task(task_id="c", command="echo c", dependencies=["a"], metadata={"weight": 1.0}))
    dag.add_task(Task(task_id="d", command="echo d", dependencies=["b", "c"], metadata={"weight": 2.0}))
    return dag


class TestTaskWeight:
    def test_str_no_marker(self):
        tw = TaskWeight(task_id="x", weight=1.0, cumulative_weight=3.0)
        assert "x" in str(tw)
        assert "heaviest" not in str(tw)

    def test_str_heaviest_marker(self):
        tw = TaskWeight(task_id="x", weight=1.0, cumulative_weight=5.0, is_heaviest=True)
        assert "[heaviest]" in str(tw)

    def test_str_shows_values(self):
        tw = TaskWeight(task_id="alpha", weight=2.5, cumulative_weight=4.5)
        s = str(tw)
        assert "2.50" in s
        assert "4.50" in s


class TestWeighResult:
    def test_has_weights_false_when_empty(self):
        wr = WeighResult(dag_name="empty")
        assert not wr.has_weights()

    def test_has_weights_true(self):
        wr = WeighResult(dag_name="d", weights=[TaskWeight("a", 1.0, 1.0)], total_weight=1.0)
        assert wr.has_weights()

    def test_heaviest_task_none_when_empty(self):
        wr = WeighResult(dag_name="d")
        assert wr.heaviest_task is None

    def test_heaviest_task_returned(self):
        heavy = TaskWeight("b", 3.0, 5.0, is_heaviest=True)
        wr = WeighResult(dag_name="d", weights=[TaskWeight("a", 1.0, 1.0), heavy], total_weight=4.0)
        assert wr.heaviest_task is heavy

    def test_str_contains_dag_name(self):
        wr = WeighResult(dag_name="mypipe", total_weight=6.0)
        assert "mypipe" in str(wr)

    def test_str_contains_total(self):
        wr = WeighResult(dag_name="d", total_weight=9.5)
        assert "9.50" in str(wr)


class TestDAGWeigher:
    def test_empty_dag_returns_empty_result(self):
        dag = DAG(name="empty")
        weigher = DAGWeigher()
        result = weigher.weigh(dag)
        assert not result.has_weights()
        assert result.total_weight == 0.0

    def test_single_task_weight(self):
        dag = DAG(name="single")
        dag.add_task(Task(task_id="only", command="echo", metadata={"weight": 5.0}))
        result = DAGWeigher().weigh(dag)
        assert len(result.weights) == 1
        assert result.total_weight == 5.0
        assert result.weights[0].is_heaviest

    def test_default_weight_is_one(self):
        dag = DAG(name="d")
        dag.add_task(Task(task_id="t", command="echo"))
        result = DAGWeigher().weigh(dag)
        assert result.weights[0].weight == 1.0

    def test_cumulative_weight_chain(self):
        dag = DAG(name="chain")
        dag.add_task(Task(task_id="a", command="echo", metadata={"weight": 2.0}))
        dag.add_task(Task(task_id="b", command="echo", dependencies=["a"], metadata={"weight": 3.0}))
        result = DAGWeigher().weigh(dag)
        by_id = {w.task_id: w for w in result.weights}
        assert by_id["b"].cumulative_weight == pytest.approx(5.0)

    def test_heaviest_task_in_diamond(self):
        dag = _build_dag()
        result = DAGWeigher().weigh(dag)
        # critical path: a(2) -> b(3) -> d(2) = 7
        assert result.heaviest_task is not None
        assert result.heaviest_task.task_id == "d"
        assert result.heaviest_task.cumulative_weight == pytest.approx(7.0)

    def test_total_weight_is_sum_of_raw(self):
        dag = _build_dag()
        result = DAGWeigher().weigh(dag)
        assert result.total_weight == pytest.approx(2.0 + 3.0 + 1.0 + 2.0)

    def test_custom_weight_key(self):
        dag = DAG(name="custom")
        dag.add_task(Task(task_id="t", command="echo", metadata={"cost": 7.0}))
        result = DAGWeigher(weight_key="cost").weigh(dag)
        assert result.weights[0].weight == pytest.approx(7.0)
