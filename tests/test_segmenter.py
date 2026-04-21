"""Tests for pipecheck.segmenter."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.segmenter import DAGSegmenter, Segment, SegmentError, SegmentResult


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    dag.add_task(Task(task_id="ingest", name="Ingest"))
    dag.add_task(Task(task_id="validate", name="Validate", dependencies=["ingest"]))
    dag.add_task(Task(task_id="transform", name="Transform", dependencies=["validate"]))
    dag.add_task(Task(task_id="load", name="Load", dependencies=["transform"]))
    return dag


class TestSegment:
    def test_str_format(self):
        t = Task(task_id="t1", name="T1")
        seg = Segment(name="seg_0_0", tasks=[t], depth_start=0, depth_end=0)
        assert "seg_0_0" in str(seg)
        assert "t1" in str(seg)

    def test_len(self):
        t = Task(task_id="t1", name="T1")
        seg = Segment(name="seg_0_0", tasks=[t], depth_start=0, depth_end=0)
        assert len(seg) == 1

    def test_len_empty(self):
        seg = Segment(name="empty", tasks=[])
        assert len(seg) == 0


class TestSegmentResult:
    def test_count_property(self):
        result = SegmentResult(dag_name="test", segments=[Segment(name="s")])
        assert result.count == 1

    def test_get_returns_segment(self):
        seg = Segment(name="seg_0_0")
        result = SegmentResult(dag_name="test", segments=[seg])
        assert result.get("seg_0_0") is seg

    def test_get_returns_none_for_missing(self):
        result = SegmentResult(dag_name="test")
        assert result.get("missing") is None

    def test_str_contains_dag_name(self):
        result = SegmentResult(dag_name="mypipe")
        assert "mypipe" in str(result)

    def test_str_shows_segment_count(self):
        seg = Segment(name="seg_0_0")
        result = SegmentResult(dag_name="pipe", segments=[seg])
        assert "1 segment" in str(result)


class TestDAGSegmenter:
    def test_single_task_dag_one_segment(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only", name="Only"))
        result = DAGSegmenter().segment(dag, size=1)
        assert result.count == 1
        assert len(result.segments[0]) == 1

    def test_chain_size_one_four_segments(self):
        dag = _build_dag()
        result = DAGSegmenter().segment(dag, size=1)
        assert result.count == 4

    def test_chain_size_two_two_segments(self):
        dag = _build_dag()
        result = DAGSegmenter().segment(dag, size=2)
        assert result.count == 2

    def test_chain_size_large_one_segment(self):
        dag = _build_dag()
        result = DAGSegmenter().segment(dag, size=10)
        assert result.count == 1
        assert len(result.segments[0]) == 4

    def test_empty_dag_returns_no_segments(self):
        dag = DAG(name="empty")
        result = DAGSegmenter().segment(dag)
        assert result.count == 0

    def test_invalid_size_raises(self):
        dag = _build_dag()
        with pytest.raises(SegmentError):
            DAGSegmenter().segment(dag, size=0)

    def test_dag_name_stored(self):
        dag = _build_dag()
        result = DAGSegmenter().segment(dag)
        assert result.dag_name == "pipeline"

    def test_segment_depth_range_correct(self):
        dag = _build_dag()
        result = DAGSegmenter().segment(dag, size=2)
        first = result.segments[0]
        assert first.depth_start == 0
        assert first.depth_end == 1
