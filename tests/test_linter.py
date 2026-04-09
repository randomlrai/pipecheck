"""Tests for pipecheck.linter module."""

import pytest
from pipecheck.dag import DAG, Task
from pipecheck.linter import DAGLinter, TaskNamingRule, TimeoutPresenceRule
from pipecheck.reporter import Severity


def _make_dag(*tasks):
    dag = DAG(dag_id="test_dag")
    for t in tasks:
        dag.add_task(t)
    return dag


class TestDAGLinter:
    def test_linter_creation(self):
        linter = DAGLinter()
        assert len(linter.rules) == 2

    def test_valid_dag_no_warnings(self):
        task = Task(task_id="process_data", timeout=30)
        dag = _make_dag(task)
        linter = DAGLinter()
        report = linter.lint(dag)
        assert report.passed
        assert len(report.entries) == 0

    def test_non_snake_case_task_id_warns(self):
        task = Task(task_id="ProcessData", timeout=30)
        dag = _make_dag(task)
        linter = DAGLinter()
        report = linter.lint(dag)
        warnings = [e for e in report.entries if e.severity == Severity.WARNING]
        assert any("snake_case" in e.message for e in warnings)

    def test_task_with_hyphen_warns(self):
        task = Task(task_id="process-data", timeout=30)
        dag = _make_dag(task)
        linter = DAGLinter()
        report = linter.lint(dag)
        assert not report.passed or any(
            "snake_case" in e.message for e in report.entries
        )

    def test_missing_timeout_warns(self):
        task = Task(task_id="load_data", timeout=None)
        dag = _make_dag(task)
        linter = DAGLinter()
        report = linter.lint(dag)
        warnings = [e for e in report.entries if e.severity == Severity.WARNING]
        assert any("timeout" in e.message for e in warnings)

    def test_multiple_tasks_multiple_warnings(self):
        t1 = Task(task_id="BadName", timeout=None)
        t2 = Task(task_id="also_bad_name", timeout=None)
        dag = _make_dag(t1, t2)
        linter = DAGLinter()
        report = linter.lint(dag)
        assert len(report.entries) >= 2

    def test_extra_rules_applied(self):
        from pipecheck.linter import LintRule
        from pipecheck.reporter import ValidationReport

        class AlwaysWarn(LintRule):
            def __init__(self):
                super().__init__("always_warn", "Always warns")

            def check(self, dag, report):
                report.add_warning(self.name, "Custom lint warning")

        task = Task(task_id="ok_task", timeout=10)
        dag = _make_dag(task)
        linter = DAGLinter(extra_rules=[AlwaysWarn()])
        report = linter.lint(dag)
        assert any("Custom lint warning" in e.message for e in report.entries)

    def test_report_dag_id_matches(self):
        dag = DAG(dag_id="my_pipeline")
        linter = DAGLinter()
        report = linter.lint(dag)
        assert report.dag_id == "my_pipeline"
