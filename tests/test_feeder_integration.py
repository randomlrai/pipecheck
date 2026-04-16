"""Integration tests for DAGFeeder with realistic pipelines."""
from pipecheck.dag import DAG, Task
from pipecheck.feeder import DAGFeeder


def _etl_dag() -> DAG:
    dag = DAG(name="etl")
    for tid, cmd in [
        ("extract", "run_extract"),
        ("validate", "run_validate"),
        ("transform", "run_transform"),
        ("load", "run_load"),
    ]:
        dag.add_task(Task(task_id=tid, command=cmd))
    dag.add_edge("extract", "validate")
    dag.add_edge("validate", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestFeederIntegration:
    def setup_method(self):
        self.dag = _etl_dag()
        self.feeder = DAGFeeder(self.dag)

    def test_feed_all_tasks(self):
        data = {
            "extract": {"owner": "alice"},
            "validate": {"owner": "bob"},
            "transform": {"owner": "carol"},
            "load": {"owner": "dave"},
        }
        result = self.feeder.feed("owners.json", data)
        assert result.count() == 4
        assert result.has_feeds()

    def test_feed_subset_of_tasks(self):
        data = {"extract": {"sla": "1h"}, "load": {"sla": "30m"}}
        result = self.feeder.feed("sla_config", data)
        assert result.count() == 2
        ids = {e.task_id for e in result.entries}
        assert ids == {"extract", "load"}

    def test_source_label_preserved(self):
        data = {"transform": {"version": "2.1"}}
        result = self.feeder.feed("manifest.yaml", data)
        assert result.entries[0].source == "manifest.yaml"

    def test_multiple_fields_per_task(self):
        data = {"extract": {"owner": "alice", "team": "data-eng", "priority": "high"}}
        result = self.feeder.feed("meta", data)
        assert result.entries[0].fields["priority"] == "high"
        assert len(result.entries[0].fields) == 3
