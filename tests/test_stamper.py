"""Tests for pipecheck.stamper."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.stamper import DAGStamper, StampEntry, StampError, StampResult


def _build_dag() -> DAG:
    dag = DAG(name="pipe")
    for tid in ("extract", "transform", "load"):
        dag.add_task(Task(task_id=tid, description=f"{tid} step"))
    dag.add_edge("extract", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestStampEntry:
    def test_str_without_author(self):
        e = StampEntry(task_id="t1", version="1.0.0", stamped_at="2024-01-01T00:00:00Z")
        assert "t1" in str(e)
        assert "1.0.0" in str(e)
        assert "2024-01-01" in str(e)
        assert "by" not in str(e)

    def test_str_with_author(self):
        e = StampEntry(task_id="t1", version="2.0", stamped_at="2024-06-01T00:00:00Z", author="alice")
        assert "alice" in str(e)
        assert "by alice" in str(e)

    def test_to_dict_without_author(self):
        e = StampEntry(task_id="t1", version="1.0", stamped_at="ts")
        d = e.to_dict()
        assert d["task_id"] == "t1"
        assert d["version"] == "1.0"
        assert "author" not in d

    def test_to_dict_with_author(self):
        e = StampEntry(task_id="t1", version="1.0", stamped_at="ts", author="bob")
        assert e.to_dict()["author"] == "bob"


class TestStampResult:
    def test_has_stamps_false_when_empty(self):
        r = StampResult(dag_name="pipe")
        assert not r.has_stamps

    def test_has_stamps_true_when_populated(self):
        r = StampResult(dag_name="pipe")
        r.entries.append(StampEntry("t", "1.0", "ts"))
        assert r.has_stamps

    def test_count_property(self):
        r = StampResult(dag_name="pipe")
        r.entries.append(StampEntry("a", "1", "ts"))
        r.entries.append(StampEntry("b", "2", "ts"))
        assert r.count == 2

    def test_get_returns_entry(self):
        r = StampResult(dag_name="pipe")
        r.entries.append(StampEntry("extract", "1.0", "ts"))
        e = r.get("extract")
        assert e is not None
        assert e.version == "1.0"

    def test_get_returns_none_for_missing(self):
        r = StampResult(dag_name="pipe")
        assert r.get("nope") is None

    def test_str_empty(self):
        r = StampResult(dag_name="pipe")
        assert "no stamps" in str(r)

    def test_str_with_entries(self):
        r = StampResult(dag_name="pipe")
        r.entries.append(StampEntry("extract", "1.0", "ts"))
        s = str(r)
        assert "1 stamp" in s
        assert "extract" in s


class TestDAGStamper:
    def test_stamp_returns_result(self):
        dag = _build_dag()
        stamper = DAGStamper()
        result = stamper.stamp(dag, {"extract": "1.0.0"})
        assert isinstance(result, StampResult)
        assert result.count == 1

    def test_stamp_multiple_tasks(self):
        dag = _build_dag()
        result = DAGStamper().stamp(dag, {"extract": "1.0", "load": "2.0"})
        assert result.count == 2

    def test_stamp_records_author(self):
        dag = _build_dag()
        result = DAGStamper().stamp(dag, {"transform": "3.1"}, author="carol")
        assert result.get("transform").author == "carol"

    def test_stamp_unknown_task_raises(self):
        dag = _build_dag()
        with pytest.raises(StampError, match="unknown_task"):
            DAGStamper().stamp(dag, {"unknown_task": "1.0"})

    def test_stamp_sets_stamped_at(self):
        dag = _build_dag()
        result = DAGStamper().stamp(dag, {"extract": "1.0"})
        assert result.get("extract").stamped_at != ""
