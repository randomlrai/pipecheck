"""Tests for pipecheck.partitioner."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.partitioner import DAGPartitioner, Partition, PartitionError, PartitionResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task("ingest", metadata={"tags": ["etl"], "team": "data"}))
    dag.add_task(Task("transform", metadata={"tags": ["etl", "ml"], "team": "data"}))
    dag.add_task(Task("train", metadata={"tags": ["ml"], "team": "ml"}))
    dag.add_task(Task("report", metadata={"team": "bi"}))  # no tags
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "train")
    return dag


class TestPartition:
    def test_str_format(self):
        p = Partition(name="etl", tasks=[Task("ingest"), Task("transform")])
        s = str(p)
        assert "etl" in s
        assert "ingest" in s
        assert "transform" in s

    def test_len(self):
        p = Partition(name="ml", tasks=[Task("train")])
        assert len(p) == 1

    def test_len_empty(self):
        p = Partition(name="empty")
        assert len(p) == 0


class TestPartitionResult:
    def test_partition_names_sorted(self):
        r = PartitionResult(dag_name="d")
        r.partitions["zzz"] = Partition("zzz")
        r.partitions["aaa"] = Partition("aaa")
        assert r.partition_names() == ["aaa", "zzz"]

    def test_has_unassigned_false(self):
        r = PartitionResult(dag_name="d")
        assert not r.has_unassigned()

    def test_has_unassigned_true(self):
        r = PartitionResult(dag_name="d", unassigned=[Task("orphan")])
        assert r.has_unassigned()

    def test_str_contains_dag_name(self):
        r = PartitionResult(dag_name="my_dag")
        assert "my_dag" in str(r)

    def test_str_shows_unassigned(self):
        r = PartitionResult(dag_name="d", unassigned=[Task("lone")])
        assert "unassigned" in str(r)
        assert "lone" in str(r)


class TestDAGPartitioner:
    def setup_method(self):
        self.dag = _build_dag()
        self.partitioner = DAGPartitioner(self.dag)

    def test_by_tag_creates_etl_partition(self):
        result = self.partitioner.by_tag()
        assert "etl" in result.partitions

    def test_by_tag_etl_contains_ingest_and_transform(self):
        result = self.partitioner.by_tag()
        ids = {t.task_id for t in result.partitions["etl"].tasks}
        assert {"ingest", "transform"} == ids

    def test_by_tag_ml_contains_transform_and_train(self):
        result = self.partitioner.by_tag()
        ids = {t.task_id for t in result.partitions["ml"].tasks}
        assert {"transform", "train"} == ids

    def test_by_tag_untagged_task_goes_to_unassigned(self):
        result = self.partitioner.by_tag()
        unassigned_ids = {t.task_id for t in result.unassigned}
        assert "report" in unassigned_ids

    def test_by_metadata_key_groups_by_team(self):
        result = self.partitioner.by_metadata_key("team")
        assert "data" in result.partitions
        assert "ml" in result.partitions
        assert "bi" in result.partitions

    def test_by_metadata_key_missing_key_unassigned(self):
        dag = DAG(name="simple")
        dag.add_task(Task("a", metadata={"team": "x"}))
        dag.add_task(Task("b"))  # no metadata key
        result = DAGPartitioner(dag).by_metadata_key("team")
        unassigned_ids = {t.task_id for t in result.unassigned}
        assert "b" in unassigned_ids

    def test_empty_dag_produces_no_partitions(self):
        dag = DAG(name="empty")
        result = DAGPartitioner(dag).by_tag()
        assert result.partitions == {}
        assert result.unassigned == []
