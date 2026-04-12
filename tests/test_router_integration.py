"""Integration tests for DAGRouter across various DAG topologies."""
from __future__ import annotations

from pipecheck.dag import DAG, Task
from pipecheck.router import DAGRouter


def _chain(n: int) -> DAG:
    """Build a linear chain: t0 -> t1 -> ... -> t{n-1}."""
    dag = DAG(name="chain")
    for i in range(n):
        deps = [f"t{i - 1}"] if i > 0 else []
        dag.add_task(Task(task_id=f"t{i}", dependencies=deps))
    return dag


def _wide_diamond() -> DAG:
    """a -> b, c, e; b -> d; c -> d; e -> d."""
    dag = DAG(name="wide_diamond")
    dag.add_task(Task(task_id="a"))
    dag.add_task(Task(task_id="b", dependencies=["a"]))
    dag.add_task(Task(task_id="c", dependencies=["a"]))
    dag.add_task(Task(task_id="e", dependencies=["a"]))
    dag.add_task(Task(task_id="d", dependencies=["b", "c", "e"]))
    return dag


class TestRouterIntegration:
    def test_chain_single_path(self):
        dag = _chain(5)
        router = DAGRouter(dag)
        result = router.route("t0", "t4")
        assert result.count() == 1
        assert result.routes[0].task_ids == ["t0", "t1", "t2", "t3", "t4"]

    def test_chain_partial_path(self):
        dag = _chain(5)
        router = DAGRouter(dag)
        result = router.route("t1", "t3")
        assert result.count() == 1
        assert result.routes[0].task_ids == ["t1", "t2", "t3"]

    def test_wide_diamond_three_paths(self):
        dag = _wide_diamond()
        router = DAGRouter(dag)
        result = router.route("a", "d")
        assert result.count() == 3

    def test_wide_diamond_shortest_is_two_hops(self):
        dag = _wide_diamond()
        router = DAGRouter(dag)
        result = router.route("a", "d")
        shortest = result.shortest()
        assert shortest is not None
        assert len(shortest) == 3  # a -> x -> d

    def test_single_node_dag_no_route_to_self(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="a"))
        dag.add_task(Task(task_id="b", dependencies=["a"]))
        router = DAGRouter(dag)
        result = router.route("b", "a")  # reverse direction
        assert not result.has_routes()

    def test_dag_name_preserved_in_result(self):
        dag = _chain(3)
        router = DAGRouter(dag)
        result = router.route("t0", "t2")
        assert result.dag_name == "chain"

    def test_source_equals_target_no_route(self):
        dag = _chain(3)
        router = DAGRouter(dag)
        result = router.route("t0", "t0")
        # A task cannot route to itself in an acyclic DAG
        assert not result.has_routes()
