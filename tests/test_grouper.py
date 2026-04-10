"""Tests for pipecheck.grouper."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.grouper import DAGGrouper, GroupError, GroupResult, TaskGroup


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task("ingest_a", metadata={"tags": ["ingest", "raw"]}))
    dag.add_task(Task("ingest_b", metadata={"tags": ["ingest"]}))
    dag.add_task(Task("transform_x", metadata={"tags": ["transform"]}))
    dag.add_task(Task("load_final", metadata={"tags": ["load"]}))
    dag.add_task(Task("orphan", metadata={}))
    return dag


class TestTaskGroup:
    def test_str_format(self):
        t = Task("my_task")
        g = TaskGroup(name="alpha", tasks=[t])
        assert "alpha" in str(g)
        assert "my_task" in str(g)

    def test_len(self):
        g = TaskGroup(name="g", tasks=[Task("a"), Task("b")])
        assert len(g) == 2


class TestGroupResult:
    def test_group_names_sorted(self):
        r = GroupResult(groups={"z": TaskGroup("z"), "a": TaskGroup("a")})
        assert r.group_names == ["a", "z"]

    def test_str_contains_group_count(self):
        r = GroupResult(groups={"g": TaskGroup("g", tasks=[Task("t1")])})
        s = str(r)
        assert "1 group" in s

    def test_str_shows_ungrouped(self):
        r = GroupResult(ungrouped=[Task("orphan")])
        assert "Ungrouped" in str(r)
        assert "orphan" in str(r)


class TestDAGGrouper:
    def setup_method(self):
        self.dag = _build_dag()
        self.grouper = DAGGrouper()

    def test_by_tag_creates_correct_groups(self):
        result = self.grouper.by_tag(self.dag)
        assert "ingest" in result.groups
        assert "transform" in result.groups
        assert "load" in result.groups

    def test_by_tag_group_sizes(self):
        result = self.grouper.by_tag(self.dag)
        assert len(result.groups["ingest"]) == 2
        assert len(result.groups["transform"]) == 1

    def test_by_tag_ungrouped_task(self):
        result = self.grouper.by_tag(self.dag)
        ungrouped_ids = [t.task_id for t in result.ungrouped]
        assert "orphan" in ungrouped_ids

    def test_by_prefix_groups_correctly(self):
        result = self.grouper.by_prefix(self.dag)
        assert "ingest" in result.groups
        assert "transform" in result.groups
        assert "load" in result.groups

    def test_by_prefix_ungrouped_when_no_separator(self):
        dag = DAG(name="simple")
        dag.add_task(Task("notag"))
        result = self.grouper.by_prefix(dag)
        assert any(t.task_id == "notag" for t in result.ungrouped)

    def test_by_prefix_empty_separator_raises(self):
        with pytest.raises(GroupError):
            self.grouper.by_prefix(self.dag, separator="")

    def test_by_prefix_custom_separator(self):
        dag = DAG(name="custom")
        dag.add_task(Task("ingest-raw"))
        dag.add_task(Task("ingest-clean"))
        result = self.grouper.by_prefix(dag, separator="-")
        assert "ingest" in result.groups
        assert len(result.groups["ingest"]) == 2
