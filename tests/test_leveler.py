"""Tests for pipecheck.leveler."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.leveler import DAGLeveler, LevelEntry, LevelError, LevelResult


def _build_dag(name="test_dag") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, task_id: str, deps=None) -> None:
    dag.add_task(Task(task_id=task_id, dependencies=deps or []))


# --- LevelEntry ---

class TestLevelEntry:
    def test_str_no_indent_at_depth_zero(self):
        entry = LevelEntry(task_id="root", depth=0)
        assert str(entry) == "[0] root"

    def test_str_indented_at_depth_two(self):
        entry = LevelEntry(task_id="leaf", depth=2)
        assert str(entry) == "    [2] leaf"


# --- LevelResult ---

class TestLevelResult:
    def test_max_depth_empty(self):
        result = LevelResult(dag_name="empty")
        assert result.max_depth == 0

    def test_max_depth_with_entries(self):
        result = LevelResult(
            dag_name="d",
            entries=[LevelEntry("a", 0), LevelEntry("b", 1), LevelEntry("c", 3)],
        )
        assert result.max_depth == 3

    def test_count_property(self):
        result = LevelResult(
            dag_name="d",
            entries=[LevelEntry("a", 0), LevelEntry("b", 1)],
        )
        assert result.count == 2

    def test_at_depth_filters_correctly(self):
        entries = [LevelEntry("a", 0), LevelEntry("b", 1), LevelEntry("c", 1)]
        result = LevelResult(dag_name="d", entries=entries)
        depth_one = result.at_depth(1)
        assert len(depth_one) == 2
        assert all(e.depth == 1 for e in depth_one)

    def test_str_contains_dag_name(self):
        result = LevelResult(dag_name="my_dag", entries=[LevelEntry("t", 0)])
        assert "my_dag" in str(result)

    def test_str_contains_max_depth(self):
        result = LevelResult(dag_name="d", entries=[LevelEntry("t", 2)])
        assert "Max depth: 2" in str(result)


# --- DAGLeveler ---

class TestDAGLeveler:
    def test_empty_dag_returns_empty_result(self):
        dag = _build_dag()
        result = DAGLeveler().level(dag)
        assert result.count == 0
        assert result.max_depth == 0

    def test_single_task_is_depth_zero(self):
        dag = _build_dag()
        _add(dag, "only")
        result = DAGLeveler().level(dag)
        assert result.count == 1
        assert result.entries[0].depth == 0

    def test_linear_chain_depths(self):
        dag = _build_dag()
        _add(dag, "a")
        _add(dag, "b", deps=["a"])
        _add(dag, "c", deps=["b"])
        result = DAGLeveler().level(dag)
        depths = {e.task_id: e.depth for e in result.entries}
        assert depths["a"] == 0
        assert depths["b"] == 1
        assert depths["c"] == 2

    def test_diamond_dag_depths(self):
        dag = _build_dag()
        _add(dag, "root")
        _add(dag, "left", deps=["root"])
        _add(dag, "right", deps=["root"])
        _add(dag, "merge", deps=["left", "right"])
        result = DAGLeveler().level(dag)
        depths = {e.task_id: e.depth for e in result.entries}
        assert depths["root"] == 0
        assert depths["left"] == 1
        assert depths["right"] == 1
        assert depths["merge"] == 2

    def test_unknown_dependency_raises_level_error(self):
        dag = _build_dag()
        _add(dag, "orphan", deps=["missing"])
        with pytest.raises(LevelError, match="unknown task"):
            DAGLeveler().level(dag)
