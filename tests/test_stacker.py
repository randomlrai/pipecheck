"""Tests for pipecheck.stacker."""
from __future__ import annotations
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.stacker import DAGStacker, StackError, StackLayer, StackResult


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    return dag


def _add(dag: DAG, task_id: str, deps: list[str] | None = None) -> None:
    dag.add_task(Task(task_id=task_id, dependencies=deps or []))


class TestStackLayer:
    def test_str_format(self):
        layer = StackLayer(index=0, task_ids=["a", "b"])
        assert "Layer 0" in str(layer)
        assert "a" in str(layer)

    def test_str_empty(self):
        layer = StackLayer(index=1)
        assert "(empty)" in str(layer)

    def test_len(self):
        layer = StackLayer(index=0, task_ids=["x", "y", "z"])
        assert len(layer) == 3


class TestStackResult:
    def test_depth_empty(self):
        result = StackResult(dag_name="dag")
        assert result.depth == 0

    def test_widest_layer_none_when_empty(self):
        result = StackResult(dag_name="dag")
        assert result.widest_layer is None

    def test_str_contains_dag_name(self):
        result = StackResult(dag_name="my_dag", layers=[StackLayer(0, ["a"])])
        assert "my_dag" in str(result)

    def test_str_contains_layer_info(self):
        result = StackResult(
            dag_name="d",
            layers=[StackLayer(0, ["a"]), StackLayer(1, ["b"])],
        )
        text = str(result)
        assert "Layer 0" in text
        assert "Layer 1" in text


class TestDAGStacker:
    def test_empty_dag_returns_empty_result(self):
        dag = _build_dag()
        result = DAGStacker().stack(dag)
        assert result.depth == 0
        assert result.layers == []

    def test_single_task_is_layer_zero(self):
        dag = _build_dag()
        _add(dag, "a")
        result = DAGStacker().stack(dag)
        assert result.depth == 1
        assert "a" in result.layers[0].task_ids

    def test_chain_produces_sequential_layers(self):
        dag = _build_dag()
        _add(dag, "a")
        _add(dag, "b", ["a"])
        _add(dag, "c", ["b"])
        result = DAGStacker().stack(dag)
        assert result.depth == 3
        assert result.layers[0].task_ids == ["a"]
        assert result.layers[1].task_ids == ["b"]
        assert result.layers[2].task_ids == ["c"]

    def test_parallel_tasks_share_layer(self):
        dag = _build_dag()
        _add(dag, "root")
        _add(dag, "left", ["root"])
        _add(dag, "right", ["root"])
        result = DAGStacker().stack(dag)
        assert result.depth == 2
        assert set(result.layers[1].task_ids) == {"left", "right"}

    def test_widest_layer_identified(self):
        dag = _build_dag()
        _add(dag, "root")
        _add(dag, "a", ["root"])
        _add(dag, "b", ["root"])
        _add(dag, "c", ["root"])
        result = DAGStacker().stack(dag)
        widest = result.widest_layer
        assert widest is not None
        assert len(widest) == 3

    def test_unknown_dependency_raises_stack_error(self):
        dag = _build_dag()
        _add(dag, "a", ["missing"])
        with pytest.raises(StackError, match="unknown task"):
            DAGStacker().stack(dag)
