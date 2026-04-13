"""Tests for pipecheck.aligner."""

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.aligner import AlignError, AlignLevel, AlignResult, DAGAligner


def _build_dag(name: str = "test_dag") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, task_id: str, deps: list[str] | None = None) -> None:
    dag.add_task(Task(task_id= or []))


# ---------------------------------------------------------------------------
# AlignLevel
# ---------------------------------------------------------------------------

class TestAlignLevel:
    def test_str_format(self):
        lvl = AlignLevel(depth=0, task_ids=["a", "b"])
        assert str(lvl) == "Level 0: [a, b]"

    def test_str_empty(self):
        lvl = AlignLevel(depth=1)
        assert "(empty)" in str(lvl)

    def test_len(self):
        lvl = AlignLevel(depth=0, task_ids=["x", "y", "z"])
        assert len(lvl) == 3


# ---------------------------------------------------------------------------
# AlignResult
# ---------------------------------------------------------------------------

class TestAlignResult:
    def test_depth_property(self):
        result = AlignResult(dag_name="d", levels=[AlignLevel(0), AlignLevel(1)])
        assert result.depth == 2

    def test_max_width(self):
        result = AlignResult(
            dag_name="d",
            levels=[
                AlignLevel(0, ["a", "b"]),
                AlignLevel(1, ["c"]),
            ],
        )
        assert result.max_width == 2

    def test_max_width_empty(self):
        result = AlignResult(dag_name="d")
        assert result.max_width == 0

    def test_str_contains_dag_name(self):
        result = AlignResult(dag_name="my_pipeline", levels=[AlignLevel(0, ["a"])])
        assert "my_pipeline" in str(result)

    def test_str_contains_level_lines(self):
        result = AlignResult(
            dag_name="d",
            levels=[AlignLevel(0, ["root"]), AlignLevel(1, ["leaf"])],
        )
        text = str(result)
        assert "Level 0" in text
        assert "Level 1" in text


# ---------------------------------------------------------------------------
# DAGAligner
# ---------------------------------------------------------------------------

class TestDAGAligner:
    def setup_method(self):
        self.aligner = DAGAligner()

    def test_empty_dag_returns_no_levels(self):
        dag = _build_dag()
        result = self.aligner.align(dag)
        assert result.depth == 0

    def test_single_task_one_level(self):
        dag = _build_dag()
        _add(dag, "only")
        result = self.aligner.align(dag)
        assert result.depth == 1
        assert result.levels[0].task_ids == ["only"]

    def test_chain_creates_sequential_levels(self):
        dag = _build_dag()
        _add(dag, "a")
        _add(dag, "b", ["a"])
        _add(dag, "c", ["b"])
        result = self.aligner.align(dag)
        assert result.depth == 3
        assert result.levels[0].task_ids == ["a"]
        assert result.levels[1].task_ids == ["b"]
        assert result.levels[2].task_ids == ["c"]

    def test_diamond_collapses_parallel_tasks(self):
        dag = _build_dag()
        _add(dag, "root")
        _add(dag, "left", ["root"])
        _add(dag, "right", ["root"])
        _add(dag, "merge", ["left", "right"])
        result = self.aligner.align(dag)
        assert result.depth == 3
        assert result.max_width == 2
        assert set(result.levels[1].task_ids) == {"left", "right"}

    def test_unknown_dependency_raises_align_error(self):
        dag = _build_dag()
        _add(dag, "orphan", ["ghost"])
        with pytest.raises(AlignError, match="unknown task"):
            self.aligner.align(dag)

    def test_result_stores_dag_name(self):
        dag = _build_dag("pipeline_x")
        _add(dag, "t")
        result = self.aligner.align(dag)
        assert result.dag_name == "pipeline_x"
