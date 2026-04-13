"""Tests for pipecheck.matcher."""
import pytest

from pipecheck.dag import DAG, Task
from pipecheck.matcher import DAGMatcher, MatchEntry, MatchError, MatchResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    for tid in ["extract_data", "extract_meta", "transform_data", "load_output"]:
        dag.add_task(Task(task_id=tid))
    return dag


class TestMatchEntry:
    def test_str_format(self):
        task = Task(task_id="extract_data")
        entry = MatchEntry(task=task, pattern="extract_*", match_type="glob")
        assert "extract_data" in str(entry)
        assert "glob" in str(entry)
        assert "extract_*" in str(entry)


class TestMatchResult:
    def test_count_property(self):
        result = MatchResult(dag_name="d", pattern="*")
        assert result.count == 0
        result.entries.append(MatchEntry(task=Task(task_id="a"), pattern="*", match_type="glob"))
        assert result.count == 1

    def test_has_matches_false_when_empty(self):
        result = MatchResult(dag_name="d", pattern="nope")
        assert not result.has_matches

    def test_has_matches_true_when_populated(self):
        result = MatchResult(dag_name="d", pattern="*")
        result.entries.append(MatchEntry(task=Task(task_id="a"), pattern="*", match_type="glob"))
        assert result.has_matches

    def test_task_ids_property(self):
        result = MatchResult(dag_name="d", pattern="*")
        result.entries.append(MatchEntry(task=Task(task_id="b"), pattern="*", match_type="glob"))
        result.entries.append(MatchEntry(task=Task(task_id="a"), pattern="*", match_type="glob"))
        assert "a" in result.task_ids
        assert "b" in result.task_ids

    def test_str_no_matches(self):
        result = MatchResult(dag_name="d", pattern="xyz")
        assert "no matches" in str(result)

    def test_str_with_matches(self):
        result = MatchResult(dag_name="d", pattern="*")
        result.entries.append(MatchEntry(task=Task(task_id="a"), pattern="*", match_type="exact"))
        s = str(result)
        assert "a" in s


class TestDAGMatcher:
    def setup_method(self):
        self.dag = _build_dag()
        self.matcher = DAGMatcher(self.dag)

    def test_glob_matches_prefix(self):
        result = self.matcher.match("extract_*", mode="glob")
        assert result.has_matches
        assert set(result.task_ids) == {"extract_data", "extract_meta"}

    def test_glob_matches_all(self):
        result = self.matcher.match("*", mode="glob")
        assert result.count == 4

    def test_exact_match(self):
        result = self.matcher.match("load_output", mode="exact")
        assert result.count == 1
        assert result.task_ids == ["load_output"]

    def test_exact_no_match(self):
        result = self.matcher.match("missing_task", mode="exact")
        assert not result.has_matches

    def test_regex_match(self):
        result = self.matcher.match(r"^(extract|load)_", mode="regex")
        assert result.count == 3

    def test_regex_invalid_raises(self):
        with pytest.raises(MatchError, match="Invalid regex"):
            self.matcher.match("[invalid", mode="regex")

    def test_unknown_mode_raises(self):
        with pytest.raises(MatchError, match="Unknown match mode"):
            self.matcher.match("*", mode="fuzzy")

    def test_results_sorted_by_task_id(self):
        result = self.matcher.match("*", mode="glob")
        assert result.task_ids == sorted(result.task_ids)
