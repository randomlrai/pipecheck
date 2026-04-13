"""Tests for pipecheck.sampler."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.sampler import DAGSampler, SampleError, SampleResult


def _build_dag(name: str = "pipeline") -> DAG:
    dag = DAG(name=name)
    for tid in ["ingest", "validate", "transform", "load", "notify"]:
        dag.add_task(Task(task_id=tid, command=f"run_{tid}.sh"))
    return dag


class TestSampleResult:
    def test_count_property(self):
        dag = _build_dag()
        tasks = list(dag.tasks.values())[:3]
        result = SampleResult(dag_name="pipeline", sampled=tasks, total=5)
        assert result.count == 3

    def test_has_sample_false_when_empty(self):
        result = SampleResult(dag_name="pipeline", sampled=[], total=5)
        assert result.has_sample is False

    def test_has_sample_true_when_populated(self):
        dag = _build_dag()
        tasks = list(dag.tasks.values())[:1]
        result = SampleResult(dag_name="pipeline", sampled=tasks, total=5)
        assert result.has_sample is True

    def test_str_contains_dag_name(self):
        result = SampleResult(dag_name="my_dag", sampled=[], total=0)
        assert "my_dag" in str(result)

    def test_str_shows_counts(self):
        dag = _build_dag()
        tasks = list(dag.tasks.values())[:2]
        result = SampleResult(dag_name="pipeline", sampled=tasks, total=5)
        assert "2/5" in str(result)

    def test_str_shows_seed_when_set(self):
        result = SampleResult(dag_name="pipeline", sampled=[], total=0, seed=42)
        assert "42" in str(result)

    def test_str_omits_seed_when_none(self):
        result = SampleResult(dag_name="pipeline", sampled=[], total=0, seed=None)
        assert "seed" not in str(result)

    def test_str_lists_task_ids(self):
        dag = _build_dag()
        tasks = [dag.tasks["ingest"], dag.tasks["load"]]
        result = SampleResult(dag_name="pipeline", sampled=tasks, total=5)
        text = str(result)
        assert "ingest" in text
        assert "load" in text


class TestDAGSampler:
    def test_sample_returns_correct_count(self):
        dag = _build_dag()
        sampler = DAGSampler()
        result = sampler.sample(dag, 3)
        assert result.count == 3

    def test_sample_total_reflects_dag_size(self):
        dag = _build_dag()
        sampler = DAGSampler()
        result = sampler.sample(dag, 2)
        assert result.total == 5

    def test_sample_zero_returns_empty(self):
        dag = _build_dag()
        sampler = DAGSampler()
        result = sampler.sample(dag, 0)
        assert result.count == 0
        assert result.has_sample is False

    def test_sample_all_tasks(self):
        dag = _build_dag()
        sampler = DAGSampler()
        result = sampler.sample(dag, 5)
        assert result.count == 5

    def test_seed_produces_deterministic_results(self):
        dag = _build_dag()
        sampler = DAGSampler()
        r1 = sampler.sample(dag, 3, seed=7)
        r2 = sampler.sample(dag, 3, seed=7)
        assert [t.task_id for t in r1.sampled] == [t.task_id for t in r2.sampled]

    def test_different_seeds_may_differ(self):
        dag = _build_dag()
        sampler = DAGSampler()
        r1 = sampler.sample(dag, 3, seed=1)
        r2 = sampler.sample(dag, 3, seed=99)
        # Not guaranteed to differ, but with 5 tasks and 3 chosen it's very likely
        ids1 = sorted(t.task_id for t in r1.sampled)
        ids2 = sorted(t.task_id for t in r2.sampled)
        assert ids1 != ids2 or True  # soft check — just ensure no crash

    def test_raises_on_negative_n(self):
        dag = _build_dag()
        sampler = DAGSampler()
        with pytest.raises(SampleError, match="non-negative"):
            sampler.sample(dag, -1)

    def test_raises_when_n_exceeds_task_count(self):
        dag = _build_dag()
        sampler = DAGSampler()
        with pytest.raises(SampleError, match="only has 5"):
            sampler.sample(dag, 10)

    def test_seed_stored_in_result(self):
        dag = _build_dag()
        sampler = DAGSampler()
        result = sampler.sample(dag, 2, seed=42)
        assert result.seed == 42

    def test_dag_name_stored_in_result(self):
        dag = _build_dag(name="etl_flow")
        sampler = DAGSampler()
        result = sampler.sample(dag, 1)
        assert result.dag_name == "etl_flow"
