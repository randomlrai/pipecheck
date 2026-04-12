from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.tracer import DAGTracer, ExecutionTrace


def _chain() -> DAG:
    dag = DAG(name="chain")
    for tid in ["a", "b", "c", "d"]:
        dag.add_task(Task(id=tid, name=tid.upper()))
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    dag.add_edge("c", "d")
    return dag


def _diamond() -> DAG:
    dag = DAG(name="diamond")
    for tid in ["start", "left", "right", "end"]:
        dag.add_task(Task(id=tid, name=tid))
    dag.add_edge("start", "left")
    dag.add_edge("start", "right")
    dag.add_edge("left", "end")
    dag.add_edge("right", "end")
    return dag


class TestTracerIntegration:
    def test_chain_trace_visits_all_downstream(self):
        dag = _chain()
        tracer = DAGTracer(dag)
        result = tracer.trace("a")
        assert isinstance(result, ExecutionTrace)
        ids = result.task_ids
        assert "a" in ids
        assert "b" in ids
        assert "c" in ids
        assert "d" in ids

    def test_chain_trace_from_middle(self):
        dag = _chain()
        tracer = DAGTracer(dag)
        result = tracer.trace("b")
        ids = result.task_ids
        assert "a" not in ids
        assert "b" in ids
        assert "c" in ids
        assert "d" in ids

    def test_chain_max_depth_limits_steps(self):
        dag = _chain()
        tracer = DAGTracer(dag)
        result = tracer.trace("a", max_depth=2)
        ids = result.task_ids
        assert "a" in ids
        assert "b" in ids
        assert "c" not in ids
        assert "d" not in ids

    def test_diamond_trace_visits_all_nodes(self):
        dag = _diamond()
        tracer = DAGTracer(dag)
        result = tracer.trace("start")
        ids = result.task_ids
        assert set(ids) == {"start", "left", "right", "end"}

    def test_leaf_task_trace_has_single_step(self):
        dag = _chain()
        tracer = DAGTracer(dag)
        result = tracer.trace("d")
        assert result.task_ids == ["d"]

    def test_str_output_contains_dag_name(self):
        dag = _chain()
        tracer = DAGTracer(dag)
        result = tracer.trace("a")
        assert "chain" in str(result)

    def test_unknown_start_task_raises(self):
        dag = _chain()
        tracer = DAGTracer(dag)
        with pytest.raises(Exception):
            tracer.trace("nonexistent")
