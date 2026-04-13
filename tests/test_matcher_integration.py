"""Integration tests for DAGMatcher across match modes."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.matcher import DAGMatcher, MatchError


def _pipeline() -> DAG:
    dag = DAG(name="pipeline")
    ids = [
        "ingest_raw",
        "ingest_clean",
        "transform_normalize",
        "transform_enrich",
        "publish_db",
        "publish_api",
    ]
    for tid in ids:
        dag.add_task(Task(task_id=tid))
    return dag


class TestMatcherIntegration:
    def setup_method(self):
        self.dag = _pipeline()
        self.matcher = DAGMatcher(self.dag)

    def test_glob_ingest_prefix_returns_two(self):
        result = self.matcher.match("ingest_*")
        assert result.count == 2
        assert set(result.task_ids) == {"ingest_raw", "ingest_clean"}

    def test_glob_wildcard_returns_all(self):
        result = self.matcher.match("*")
        assert result.count == 6

    def test_glob_no_match_returns_empty(self):
        result = self.matcher.match("archive_*")
        assert not result.has_matches

    def test_exact_single_task(self):
        result = self.matcher.match("publish_db", mode="exact")
        assert result.count == 1
        assert result.task_ids == ["publish_db"]

    def test_regex_underscore_split(self):
        result = self.matcher.match(r"^(transform|publish)_", mode="regex")
        assert result.count == 4

    def test_regex_suffix_match(self):
        result = self.matcher.match(r"_api$", mode="regex")
        assert result.count == 1
        assert result.task_ids == ["publish_api"]

    def test_match_result_sorted(self):
        result = self.matcher.match("*")
        assert result.task_ids == sorted(result.task_ids)

    def test_match_entry_match_type_recorded(self):
        result = self.matcher.match("ingest_raw", mode="exact")
        assert result.entries[0].match_type == "exact"
