"""Tests for pipecheck.scheduler."""
from __future__ import annotations
import pytest
from pipecheck.dag import DAG, Task
from pipecheck.scheduler import DAGScheduler, ExecutionSchedule, ScheduleWave


def _build_dag(name: str = "test") -> DAG:
    dag = DAG(name=name)
    t1 = Task(task_id="ingest", command="load", timeout=10)
    t2 = Task(task_id="transform", command="clean", timeout=20)
    t3 = Task(task_id="export", command="write", timeout=5)
    t4 = Task(task_id="notify", command="send", timeout=2)
    dag.add_task(t1)
    dag.add_task(t2)
    dag.add_task(t3)
    dag.add_task(t4)
    dag.add_edge("ingest", "transform")
    dag.add_edge("transform", "export")
    dag.add_edge("export", "notify")
    return dag


class TestScheduleWave:
    def test_str_format(self):
        t = Task(task_id="a", command="run")
        wave = ScheduleWave(index=0, tasks=[t])
        assert "Wave 0" in str(wave)
        assert "a" in str(wave)

    def test_len(self):
        t1 = Task(task_id="a", command="run")
        t2 = Task(task_id="b", command="run")
        wave = ScheduleWave(index=1, tasks=[t1, t2])
        assert len(wave) == 2


class TestExecutionSchedule:
    def test_total_waves(self):
        sched = ExecutionSchedule(dag_name="mydag")
        sched.waves.append(ScheduleWave(index=0, tasks=[]))
        sched.waves.append(ScheduleWave(index=1, tasks=[]))
        assert sched.total_waves == 2

    def test_total_tasks(self):
        t = Task(task_id="x", command="run")
        sched = ExecutionSchedule(dag_name="mydag")
        sched.waves.append(ScheduleWave(index=0, tasks=[t, t]))
        sched.waves.append(ScheduleWave(index=1, tasks=[t]))
        assert sched.total_tasks == 3

    def test_str_contains_dag_name(self):
        sched = ExecutionSchedule(dag_name="pipeline_x")
        assert "pipeline_x" in str(sched)


class TestDAGScheduler:
    def test_scheduler_creation(self):
        dag = _build_dag()
        scheduler = DAGScheduler(dag)
        assert scheduler is not None

    def test_linear_dag_produces_sequential_waves(self):
        dag = _build_dag()
        scheduler = DAGScheduler(dag)
        schedule = scheduler.build_schedule()
        assert schedule.total_waves == 4
        assert schedule.total_tasks == 4

    def test_parallel_tasks_in_same_wave(self):
        dag = DAG(name="parallel")
        root = Task(task_id="root", command="start")
        left = Task(task_id="left", command="run")
        right = Task(task_id="right", command="run")
        dag.add_task(root)
        dag.add_task(left)
        dag.add_task(right)
        dag.add_edge("root", "left")
        dag.add_edge("root", "right")
        scheduler = DAGScheduler(dag)
        schedule = scheduler.build_schedule()
        assert schedule.total_waves == 2
        assert len(schedule.waves[1]) == 2

    def test_single_task_dag(self):
        dag = DAG(name="solo")
        dag.add_task(Task(task_id="only", command="do"))
        scheduler = DAGScheduler(dag)
        schedule = scheduler.build_schedule()
        assert schedule.total_waves == 1
        assert schedule.total_tasks == 1

    def test_critical_path_linear(self):
        dag = _build_dag()
        scheduler = DAGScheduler(dag)
        path = scheduler.critical_path()
        assert path[0] == "ingest"
        assert path[-1] == "notify"
        assert len(path) == 4

    def test_critical_path_picks_longer_branch(self):
        dag = DAG(name="branch")
        root = Task(task_id="root", command="r", timeout=1)
        fast = Task(task_id="fast", command="f", timeout=1)
        slow = Task(task_id="slow", command="s", timeout=100)
        end = Task(task_id="end", command="e", timeout=1)
        dag.add_task(root)
        dag.add_task(fast)
        dag.add_task(slow)
        dag.add_task(end)
        dag.add_edge("root", "fast")
        dag.add_edge("root", "slow")
        dag.add_edge("fast", "end")
        dag.add_edge("slow", "end")
        scheduler = DAGScheduler(dag)
        path = scheduler.critical_path()
        assert "slow" in path
