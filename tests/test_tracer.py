"""Tests for pipecheck.tracer."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.tracer import DAGTracer, ExecutionTrace, TraceStep


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    for tid in ("extract", "transform", "load", "notify"):
        dag.add_task(Task(task_id=tid))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    dag.add_edge("load", "notify")
    return dag


class TestTraceStep:
    def test_str_no_indent(self):
        t = Task(task_id="extract")
        step = TraceStep(task=t, depth=0)
        assert str(step) == "-> extract"

    def test_str_indented(self):
        t = Task(task_id="load")
        step = TraceStep(task=t, depth=2)
        assert str(step).startswith("    ->")


class TestExecutionTrace:
    def test_task_ids_property(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        trace = tracer.trace_from("extract")
        assert trace.task_ids == ["extract", "transform", "load", "notify"]

    def test_str_contains_start(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        trace = tracer.trace_from("extract")
        assert "Trace from 'extract'" in str(trace)


class TestDAGTracer:
    def test_tracer_creation(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        assert tracer._dag is dag

    def test_trace_from_full_chain(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        trace = tracer.trace_from("extract")
        assert isinstance(trace, ExecutionTrace)
        assert trace.task_ids == ["extract", "transform", "load", "notify"]

    def test_trace_from_mid_chain(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        trace = tracer.trace_from("transform")
        assert "extract" not in trace.task_ids
        assert trace.task_ids[0] == "transform"

    def test_trace_unknown_start_raises(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        with pytest.raises(KeyError, match="missing"):
            tracer.trace_from("missing")

    def test_trace_unknown_end_raises(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        with pytest.raises(KeyError, match="ghost"):
            tracer.trace_from("extract", end_id="ghost")

    def test_critical_path_full(self):
        dag = _build_dag()
        tracer = DAGTracer(dag)
        path = tracer.critical_path()
        assert path == ["extract", "transform", "load", "notify"]

    def test_critical_path_empty_dag(self):
        dag = DAG(name="empty")
        tracer = DAGTracer(dag)
        assert tracer.critical_path() == []

    def test_critical_path_branching(self):
        dag = DAG(name="branching")
        for tid in ("a", "b", "c", "d"):
            dag.add_task(Task(task_id=tid))
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("c", "d")
        tracer = DAGTracer(dag)
        path = tracer.critical_path()
        assert len(path) == 3
        assert path[0] == "a"
        assert path[-1] == "d"
