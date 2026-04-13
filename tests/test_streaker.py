"""Tests for pipecheck.streaker."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.streaker import DAGStreaker, Streak, StreakResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, task_id: str, **kwargs) -> None:
    dag.add_task(Task(id=task_id, **kwargs))


def _edge(dag: DAG, src: str, dst: str) -> None:
    dag.add_edge(src, dst)


class TestStreak:
    def test_str_empty(self):
        s = Streak()
        assert str(s) == "Streak(empty)"

    def test_str_single(self):
        s = Streak(task_ids=["a"])
        assert str(s) == "a"

    def test_str_chain(self):
        s = Streak(task_ids=["a", "b", "c"])
        assert str(s) == "a -> b -> c"

    def test_len(self):
        s = Streak(task_ids=["x", "y"])
        assert len(s) == 2

    def test_len_empty(self):
        assert len(Streak()) == 0


class TestStreakResult:
    def test_str_format(self):
        result = StreakResult(dag_name="pipe", streaks=[Streak(["a", "b"])])
        s = str(result)
        assert "pipe" in s
        assert "streaks=1" in s
        assert "longest=2" in s

    def test_longest_empty_result(self):
        result = StreakResult(dag_name="empty")
        assert len(result.longest) == 0

    def test_count_property(self):
        result = StreakResult(dag_name="d", streaks=[Streak(["a"]), Streak(["b", "c"])])
        assert result.count == 2

    def test_longest_picks_max(self):
        short = Streak(["a"])
        long_ = Streak(["b", "c", "d"])
        result = StreakResult(dag_name="d", streaks=[short, long_])
        assert result.longest is long_


class TestDAGStreaker:
    def test_empty_dag_returns_no_streaks(self):
        dag = _build_dag()
        result = DAGStreaker().find(dag)
        assert result.count == 0
        assert result.dag_name == "test"

    def test_single_task_is_one_streak(self):
        dag = _build_dag()
        _add(dag, "a")
        result = DAGStreaker().find(dag)
        assert result.count == 1
        assert len(result.longest) == 1

    def test_linear_chain_is_single_streak(self):
        dag = _build_dag()
        for t in ["a", "b", "c"]:
            _add(dag, t)
        _edge(dag, "a", "b")
        _edge(dag, "b", "c")
        result = DAGStreaker().find(dag)
        assert result.count == 1
        assert len(result.longest) == 3
        assert result.longest.task_ids == ["a", "b", "c"]

    def test_diamond_splits_into_multiple_streaks(self):
        dag = _build_dag()
        for t in ["root", "left", "right", "end"]:
            _add(dag, t)
        _edge(dag, "root", "left")
        _edge(dag, "root", "right")
        _edge(dag, "left", "end")
        _edge(dag, "right", "end")
        result = DAGStreaker().find(dag)
        # root, left, right, end all have branching — each is its own streak
        assert result.count >= 2

    def test_dag_name_stored(self):
        dag = _build_dag(name="my_pipeline")
        _add(dag, "t1")
        result = DAGStreaker().find(dag)
        assert result.dag_name == "my_pipeline"
