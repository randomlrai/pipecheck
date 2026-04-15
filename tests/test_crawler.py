"""Tests for pipecheck.crawler."""
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.crawler import CrawlError, CrawlStep, CrawlResult, DAGCrawler


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    for tid in ("ingest", "clean", "transform", "load"):
        dag.add_task(Task(id=tid))
    dag.add_edge("ingest", "clean")
    dag.add_edge("clean", "transform")
    dag.add_edge("transform", "load")
    return dag


class TestCrawlStep:
    def test_str_no_indent(self):
        step = CrawlStep(task_id="ingest", depth=0)
        assert str(step) == "ingest"

    def test_str_indented(self):
        step = CrawlStep(task_id="clean", depth=2)
        assert str(step) == "    clean"

    def test_str_with_parent(self):
        step = CrawlStep(task_id="clean", depth=1, parent_id="ingest")
        assert "parent: ingest" in str(step)

    def test_str_no_parent_info_when_none(self):
        step = CrawlStep(task_id="ingest", depth=0, parent_id=None)
        assert "parent" not in str(step)


class TestCrawlResult:
    def test_count_property(self):
        result = CrawlResult(dag_name="dag")
        result.steps.append(CrawlStep(task_id="a", depth=0))
        assert result.count == 1

    def test_task_ids_property(self):
        result = CrawlResult(dag_name="dag")
        result.steps.append(CrawlStep(task_id="a", depth=0))
        result.steps.append(CrawlStep(task_id="b", depth=1))
        assert result.task_ids == ["a", "b"]

    def test_max_depth_empty(self):
        result = CrawlResult(dag_name="dag")
        assert result.max_depth == 0

    def test_max_depth_non_empty(self):
        result = CrawlResult(dag_name="dag")
        result.steps.append(CrawlStep(task_id="a", depth=0))
        result.steps.append(CrawlStep(task_id="b", depth=3))
        assert result.max_depth == 3

    def test_str_contains_dag_name(self):
        result = CrawlResult(dag_name="my_pipeline")
        assert "my_pipeline" in str(result)

    def test_str_contains_count(self):
        result = CrawlResult(dag_name="dag")
        result.steps.append(CrawlStep(task_id="a", depth=0))
        assert "1 task" in str(result)


class TestDAGCrawler:
    def test_crawl_visits_all_tasks(self):
        dag = _build_dag()
        crawler = DAGCrawler()
        result = crawler.crawl(dag)
        assert set(result.task_ids) == {"ingest", "clean", "transform", "load"}

    def test_crawl_root_has_depth_zero(self):
        dag = _build_dag()
        crawler = DAGCrawler()
        result = crawler.crawl(dag)
        root_step = next(s for s in result.steps if s.task_id == "ingest")
        assert root_step.depth == 0

    def test_crawl_depth_increases_along_chain(self):
        dag = _build_dag()
        crawler = DAGCrawler()
        result = crawler.crawl(dag)
        depths = {s.task_id: s.depth for s in result.steps}
        assert depths["ingest"] < depths["clean"] < depths["transform"] < depths["load"]

    def test_crawl_from_start_id(self):
        dag = _build_dag()
        crawler = DAGCrawler()
        result = crawler.crawl(dag, start_id="clean")
        assert "ingest" not in result.task_ids
        assert "clean" in result.task_ids
        assert "load" in result.task_ids

    def test_crawl_invalid_start_raises(self):
        dag = _build_dag()
        crawler = DAGCrawler()
        with pytest.raises(CrawlError, match="not found"):
            crawler.crawl(dag, start_id="missing_task")

    def test_crawl_diamond_dag_no_duplicates(self):
        dag = DAG(name="diamond")
        for tid in ("a", "b", "c", "d"):
            dag.add_task(Task(id=tid))
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        crawler = DAGCrawler()
        result = crawler.crawl(dag)
        assert len(result.task_ids) == len(set(result.task_ids))
        assert set(result.task_ids) == {"a", "b", "c", "d"}
