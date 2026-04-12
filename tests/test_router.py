"""Tests for pipecheck.router."""
from __future__ import annotations

import pytest

from pipecheck.dag import DAG, Task
from pipecheck.router import DAGRouter, Route, RouteError, RouteResult


def _build_dag() -> DAG:
    dag = DAG(name="test_dag")
    dag.add_task(Task(task_id="a"))
    dag.add_task(Task(task_id="b", dependencies=["a"]))
    dag.add_task(Task(task_id="c", dependencies=["a"]))
    dag.add_task(Task(task_id="d", dependencies=["b", "c"]))
    return dag


class TestRoute:
    def test_str_format(self):
        r = Route(task_ids=["a", "b", "d"])
        assert str(r) == "a -> b -> d"

    def test_len(self):
        r = Route(task_ids=["a", "b"])
        assert len(r) == 2

    def test_len_empty(self):
        r = Route()
        assert len(r) == 0


class TestRouteResult:
    def test_has_routes_false_when_empty(self):
        rr = RouteResult(dag_name="d", source="a", target="b")
        assert not rr.has_routes()

    def test_has_routes_true_when_populated(self):
        rr = RouteResult(
            dag_name="d",
            source="a",
            target="b",
            routes=[Route(task_ids=["a", "b"])],
        )
        assert rr.has_routes()

    def test_count(self):
        rr = RouteResult(
            dag_name="d",
            source="a",
            target="d",
            routes=[
                Route(task_ids=["a", "b", "d"]),
                Route(task_ids=["a", "c", "d"]),
            ],
        )
        assert rr.count() == 2

    def test_shortest_returns_none_when_empty(self):
        rr = RouteResult(dag_name="d", source="a", target="b")
        assert rr.shortest() is None

    def test_longest_returns_none_when_empty(self):
        rr = RouteResult(dag_name="d", source="a", target="b")
        assert rr.longest() is None

    def test_shortest_picks_minimum(self):
        short = Route(task_ids=["a", "d"])
        long_ = Route(task_ids=["a", "b", "c", "d"])
        rr = RouteResult(dag_name="d", source="a", target="d", routes=[long_, short])
        assert rr.shortest() is short

    def test_longest_picks_maximum(self):
        short = Route(task_ids=["a", "d"])
        long_ = Route(task_ids=["a", "b", "c", "d"])
        rr = RouteResult(dag_name="d", source="a", target="d", routes=[short, long_])
        assert rr.longest() is long_

    def test_str_contains_dag_name(self):
        rr = RouteResult(dag_name="my_dag", source="a", target="d")
        assert "my_dag" in str(rr)

    def test_str_contains_route_count(self):
        rr = RouteResult(
            dag_name="d",
            source="a",
            target="b",
            routes=[Route(task_ids=["a", "b"])],
        )
        assert "1 path" in str(rr)


class TestDAGRouter:
    def test_raises_for_unknown_source(self):
        dag = _build_dag()
        router = DAGRouter(dag)
        with pytest.raises(RouteError, match="Source task"):
            router.route("x", "d")

    def test_raises_for_unknown_target(self):
        dag = _build_dag()
        router = DAGRouter(dag)
        with pytest.raises(RouteError, match="Target task"):
            router.route("a", "z")

    def test_finds_two_paths_in_diamond(self):
        dag = _build_dag()
        router = DAGRouter(dag)
        result = router.route("a", "d")
        assert result.count() == 2

    def test_no_route_returns_empty(self):
        dag = DAG(name="simple")
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b"))
        router = DAGRouter(dag)
        result = router.route("a", "b")
        assert not result.has_routes()

    def test_direct_edge_found(self):
        dag = DAG(name="simple")
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        router = DAGRouter(dag)
        result = router.route("a", "b")
        assert result.has_routes()
        assert result.count() == 1
        assert result.routes[0].task_ids == ["a", "b"]
