"""Tests for pipecheck.feeder."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.feeder import DAGFeeder, FeedEntry, FeedError, FeedResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="ingest", command="cmd_a"))
    dag.add_task(Task(task_id="transform", command="cmd_b"))
    dag.add_task(Task(task_id="load", command="cmd_c"))
    return dag


class TestFeedEntry:
    def test_str_format(self):
        e = FeedEntry(task_id="ingest", source="db", fields={"owner": "alice"})
        out = str(e)
        assert "ingest <- db" in out
        assert "owner: alice" in out

    def test_str_no_fields(self):
        e = FeedEntry(task_id="load", source="csv")
        assert str(e) == "load <- csv"


class TestFeedResult:
    def test_has_feeds_false_when_empty(self):
        r = FeedResult(dag_name="d")
        assert not r.has_feeds()

    def test_has_feeds_true_when_populated(self):
        r = FeedResult(dag_name="d", entries=[FeedEntry("t", "s")])
        assert r.has_feeds()

    def test_count_property(self):
        r = FeedResult(dag_name="d", entries=[FeedEntry("a", "s"), FeedEntry("b", "s")])
        assert r.count() == 2

    def test_str_empty(self):
        r = FeedResult(dag_name="mypipe")
        assert "no feeds" in str(r)

    def test_str_non_empty(self):
        r = FeedResult(dag_name="mypipe", entries=[FeedEntry("ingest", "db")])
        s = str(r)
        assert "mypipe" in s
        assert "1 feed" in s


class TestDAGFeeder:
    def test_feed_applies_to_existing_task(self):
        dag = _build_dag()
        feeder = DAGFeeder(dag)
        result = feeder.feed("metadata.json", {"ingest": {"owner": "bob"}})
        assert result.count() == 1
        assert result.entries[0].task_id == "ingest"
        assert result.entries[0].fields["owner"] == "bob"

    def test_feed_multiple_tasks(self):
        dag = _build_dag()
        feeder = DAGFeeder(dag)
        result = feeder.feed("src", {"ingest": {"k": "v"}, "load": {"k2": "v2"}})
        assert result.count() == 2

    def test_feed_unknown_task_raises(self):
        dag = _build_dag()
        feeder = DAGFeeder(dag)
        with pytest.raises(FeedError, match="ghost"):
            feeder.feed("src", {"ghost": {"x": "y"}})

    def test_feed_empty_data_returns_empty_result(self):
        dag = _build_dag()
        feeder = DAGFeeder(dag)
        result = feeder.feed("src", {})
        assert not result.has_feeds()
