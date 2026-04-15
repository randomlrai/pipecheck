"""Tests for pipecheck.mirrorer."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.mirrorer import DAGMirrorer, MirrorEntry, MirrorResult


def _build_dag() -> DAG:
    dag = DAG(name="pipeline")
    for tid in ("ingest", "transform", "load"):
        dag.add_task(Task(task_id=tid, description=f"{tid} step"))
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestMirrorEntry:
    def test_str_format(self):
        entry = MirrorEntry(original_id="ingest", mirrored_id="mirror_ingest")
        assert str(entry) == "ingest -> mirror_ingest"


class TestMirrorResult:
    def _make_result(self) -> MirrorResult:
        dag = DAG(name="empty")
        return MirrorResult(dag_name="empty", dag=dag, entries=[])

    def test_count_empty(self):
        result = self._make_result()
        assert result.count == 0

    def test_has_mappings_false_when_empty(self):
        result = self._make_result()
        assert result.has_mappings is False

    def test_has_mappings_true_when_populated(self):
        dag = DAG(name="x")
        result = MirrorResult(
            dag_name="x",
            dag=dag,
            entries=[MirrorEntry("a", "mirror_a")],
        )
        assert result.has_mappings is True

    def test_mapping_dict(self):
        dag = DAG(name="x")
        entries = [
            MirrorEntry("a", "mirror_a"),
            MirrorEntry("b", "mirror_b"),
        ]
        result = MirrorResult(dag_name="x", dag=dag, entries=entries)
        assert result.mapping() == {"a": "mirror_a", "b": "mirror_b"}

    def test_str_contains_dag_name(self):
        dag = DAG(name="pipeline")
        entries = [MirrorEntry("ingest", "mirror_ingest")]
        result = MirrorResult(dag_name="pipeline", dag=dag, entries=entries)
        assert "pipeline" in str(result)

    def test_str_contains_entry(self):
        dag = DAG(name="pipeline")
        entries = [MirrorEntry("ingest", "mirror_ingest")]
        result = MirrorResult(dag_name="pipeline", dag=dag, entries=entries)
        assert "ingest -> mirror_ingest" in str(result)


class TestDAGMirrorer:
    def setup_method(self):
        self.dag = _build_dag()
        self.mirrorer = DAGMirrorer()

    def test_default_prefix_applied(self):
        result = self.mirrorer.mirror(self.dag)
        assert all(mid.startswith("mirror_") for mid in result.dag.tasks)

    def test_custom_prefix(self):
        result = self.mirrorer.mirror(self.dag, prefix="copy_")
        assert "copy_ingest" in result.dag.tasks

    def test_suffix_applied(self):
        result = self.mirrorer.mirror(self.dag, prefix=None, suffix="_v2")
        assert "ingest_v2" in result.dag.tasks

    def test_edges_remapped(self):
        result = self.mirrorer.mirror(self.dag, prefix="m_")
        assert ("m_ingest", "m_transform") in result.dag.edges
        assert ("m_transform", "m_load") in result.dag.edges

    def test_task_count_preserved(self):
        result = self.mirrorer.mirror(self.dag)
        assert len(result.dag.tasks) == len(self.dag.tasks)

    def test_mirrored_dag_name(self):
        result = self.mirrorer.mirror(self.dag)
        assert result.dag.name == "pipeline_mirror"

    def test_result_dag_name_is_original(self):
        result = self.mirrorer.mirror(self.dag)
        assert result.dag_name == "pipeline"

    def test_entry_count_matches_tasks(self):
        result = self.mirrorer.mirror(self.dag)
        assert result.count == len(self.dag.tasks)
