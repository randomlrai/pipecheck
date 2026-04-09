"""DAG health scorer — produces a 0-100 score based on validation results."""

from dataclasses import dataclass, field
from typing import List

from pipecheck.reporter import ValidationReport, Severity


# Penalty points deducted per issue
_PENALTY = {
    Severity.ERROR: 25,
    Severity.WARNING: 5,
    Severity.INFO: 1,
}

MAX_SCORE = 100


@dataclass
class ScoreBreakdown:
    """Detailed breakdown of how the final score was calculated."""

    base_score: int = MAX_SCORE
    penalties: List[str] = field(default_factory=list)
    final_score: int = MAX_SCORE

    def __str__(self) -> str:  # pragma: no cover
        lines = [f"Score: {self.final_score}/{MAX_SCORE}"]
        if self.penalties:
            lines.append("Penalties:")
            for p in self.penalties:
                lines.append(f"  {p}")
        return "\n".join(lines)


class DAGScorer:
    """Computes a health score for a DAG from its ValidationReport."""

    def score(self, report: ValidationReport) -> ScoreBreakdown:
        """Return a ScoreBreakdown for *report*."""
        breakdown = ScoreBreakdown()
        total_penalty = 0

        for entry in report.entries:
            penalty = _PENALTY.get(entry.severity, 0)
            if penalty:
                total_penalty += penalty
                breakdown.penalties.append(
                    f"-{penalty} [{entry.severity.value.upper()}] {entry.message}"
                )

        breakdown.final_score = max(0, MAX_SCORE - total_penalty)
        return breakdown

    def grade(self, score: int) -> str:
        """Convert a numeric score to a letter grade."""
        if score >= 90:
            return "A"
        if score >= 75:
            return "B"
        if score >= 60:
            return "C"
        if score >= 40:
            return "D"
        return "F"
