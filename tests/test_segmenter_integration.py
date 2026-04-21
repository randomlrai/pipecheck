"""Integration tests for DAGSegmenter against realistic pipeline shapes."""
from pipecheck.dag import DAG, Task
from pipecheck.segmenter import DAGSegmenter


def _diamond() -> DAG:
    """A -> B, A -> C, B -> D, C -> D."""
    dag = DAG(name="diamond")
    for tid in ("a", "b", "c", "d"):
        deps = {
            "a": [],
            "b": ["a"],
            "c": ["a"],
            "d": ["b", "c"],
        }[tid]
        dag.add_task(Task(task_id=tid, name=tid.upper(), dependencies=deps))
    return dag


def _wide() -> DAG:
    """root -> (w1, w2, w3) -> sink."""
    dag = DAG(name="wide")
    dag.add_task(Task(task_id="root", name="Root"))
    for i in (1, 2, 3):
        dag.add_task(Task(task_id=f"w{i}", name=f"W{i}", dependencies=["root"]))
    dag.add_task(
        Task(task_id="sink", name="Sink", dependencies=["w1", "w2", "w3"])
    )
    return dag


class TestSegmenterIntegration:
    def test_diamond_size_one_three_segments(self):
        result = DAGSegmenter().segment(_diamond(), size=1)
        # depths: a=0, b=1, c=1, d=2  => 3 distinct depth levels
        assert result.count == 3

    def test_diamond_root_in_first_segment(self):
        result = DAGSegmenter().segment(_diamond(), size=1)
        first = result.segments[0]
        assert any(t.task_id == "a" for t in first.tasks)

    def test_diamond_leaf_in_last_segment(self):
        result = DAGSegmenter().segment(_diamond(), size=1)
        last = result.segments[-1]
        assert any(t.task_id == "d" for t in last.tasks)

    def test_wide_middle_segment_has_three_tasks(self):
        result = DAGSegmenter().segment(_wide(), size=1)
        # depths: root=0, w1/w2/w3=1, sink=2
        middle = result.segments[1]
        assert len(middle) == 3

    def test_wide_size_two_two_segments(self):
        result = DAGSegmenter().segment(_wide(), size=2)
        assert result.count == 2

    def test_all_tasks_covered(self):
        dag = _diamond()
        result = DAGSegmenter().segment(dag, size=1)
        covered = {t.task_id for seg in result.segments for t in seg.tasks}
        assert covered == set(dag.tasks.keys())
