"""DAG linting rules for naming conventions and structural best practices."""

from dataclasses import dataclass, field
from typing import List
from pipecheck.dag import DAG
from pipecheck.reporter import ValidationReport, Severity


@dataclass
class LintRule:
    name: str
    description: str

    def check(self, dag: DAG, report: ValidationReport) -> None:
        raise NotImplementedError


class TaskNamingRule(LintRule):
    """Task IDs should be snake_case and non-empty."""

    def __init__(self):
        super().__init__(
            name="task_naming",
            description="Task IDs must be non-empty snake_case strings",
        )

    def check(self, dag: DAG, report: ValidationReport) -> None:
        import re
        pattern = re.compile(r'^[a-z][a-z0-9_]*$')
        for task in dag.tasks.values():
            if not task.task_id:
                report.add_error(self.name, "Task has an empty ID")
            elif not pattern.match(task.task_id):
                report.add_warning(
                    self.name,
                    f"Task '{task.task_id}' does not follow snake_case convention",
                )


class TimeoutPresenceRule(LintRule):
    """All tasks should define a timeout."""

    def __init__(self):
        super().__init__(
            name="timeout_presence",
            description="All tasks should specify a timeout value",
        )

    def check(self, dag: DAG, report: ValidationReport) -> None:
        for task in dag.tasks.values():
            if task.timeout is None:
                report.add_warning(
                    self.name,
                    f"Task '{task.task_id}' has no timeout defined",
                )


class DAGLinter:
    """Runs a collection of lint rules against a DAG."""

    DEFAULT_RULES: List[LintRule] = []

    def __init__(self, extra_rules: List[LintRule] = None):
        self.rules: List[LintRule] = [
            TaskNamingRule(),
            TimeoutPresenceRule(),
        ]
        if extra_rules:
            self.rules.extend(extra_rules)

    def lint(self, dag: DAG) -> ValidationReport:
        report = ValidationReport(dag_id=dag.dag_id)
        for rule in self.rules:
            rule.check(dag, report)
        return report
