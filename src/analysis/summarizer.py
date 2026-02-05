"""Weekly experiment summarization and analysis using Claude API.

Stage 1: Analyze this week's experiments with full historical context.
Stage 3: Update cumulative learnings after analysis.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.analysis.claude_client import ClaudeClient, SYSTEM_PROMPT_SCIENTIST
from src.config import CUMULATIVE_LEARNINGS_PATH, KNOWLEDGE_DIR
from src.models.data import (
    AnalysisResult,
    CumulativeLearnings,
    Experiment,
    JournalEntry,
    WeeklyData,
)
from src.parsers.experiment_sheet import experiment_to_summary_text
from src.parsers.journal import entries_to_summary_text

logger = logging.getLogger(__name__)

ANALYSIS_PROMPT = """## Institutional Knowledge (project history)
{project_arc}

## This Week's Experiment Data
{experiment_data}

## Cumulative Learnings (from recent weeks)
{cumulative_learnings}

## Team Journal Entries (this week)
{journal_entries}

## Your Analysis Tasks:

1. EXPERIMENT FAMILY CLASSIFICATION
   - For each experiment this week, identify its family/series
   - Classify as: NEW (first time investigating this question), \
CONTINUATION (same setup as a previous experiment), or \
MODIFICATION (changed one or more variables from a previous experiment)
   - If a continuation/modification, reference the previous experiment(s)
   - In sparse weeks (1-3 experiments), focus on lineage rather than statistics

2. EXECUTIVE SUMMARY (for leadership)
   - 3-5 bullet points covering the week's most important findings
   - Written for a non-scientist audience (clear, jargon-minimized)

3. EXPERIMENT-BY-EXPERIMENT ANALYSIS (for scientists)
   - For each experiment, evaluate:
     - Did the experiment achieve its stated purpose?
     - What do the Ct values tell us? (ONLY compare within same experiment family)
     - If continuation/modification: how do results compare to previous runs \
in this series? Did the variable change improve or worsen performance?
     - Quality assessment: consistent replicates? controls amplifying?
   - DO NOT compare Ct values across different experiment families/setups

4. CONTRADICTION & ANOMALY CHECK
   - Flag any results that contradict previous findings or expected behavior
   - Flag unexpected Ct values (unusually high, unusually low, or missing)
   - Flag when scientist Resume conclusions don't match the raw data
   - For each flag, explain why it might have occurred and whether it needs follow-up

5. CROSS-EXPERIMENT INSIGHTS
   - Identify patterns or trends across this week's experiments
   - Note any emerging conclusions about assay/polymerase/sample combinations

6. UPDATED CUMULATIVE LEARNINGS
   - Return an updated version of the cumulative learnings as a JSON block \
wrapped in ```json ... ``` with these fields:
     - key_learnings: list of strings (add new, keep valid existing ones)
     - open_questions: list of strings (remove answered, add new)
     - experiment_history_summary: dict with experiment family summaries
     - goal_progress: dict with goal status updates"""

CONSTRAINT_EXTRACTION_PROMPT = """Read the following team journal entries and meeting minutes from this week.
Extract practical constraints for experiment planning:

1. DEVICE STATUS: Which devices (TS-003, TS-005, TS-006, etc.) are:
   - Working normally (used in recent experiments)
   - Having issues (debugging, firmware updates, hardware problems)
   - Being modified or updated

2. SCIENTIST AVAILABILITY: Based on journal entries, who is:
   - Actively running experiments (available for more)
   - Focused on engineering/hardware tasks (likely busy)
   - Mentioned as absent or on other projects

3. CONSUMABLE/REAGENT STATUS:
   - Any reagent batches mentioned as running low or being ordered
   - New reagent batches arriving
   - Consumable issues (cartridge assembly problems, vial issues)

4. BLOCKERS & DEPENDENCIES:
   - Anything the team is waiting on (parts, approvals, external samples)
   - Firmware or software updates needed before certain experiments
   - Equipment being repaired or calibrated

Return as structured JSON wrapped in ```json ... ```.

## Journal Entries
{journal_text}"""


def load_project_arc() -> str:
    """Load the project arc narrative from the knowledge base."""
    arc_path = KNOWLEDGE_DIR / "project_arc.json"
    if arc_path.exists():
        arc_data = json.loads(arc_path.read_text(encoding="utf-8"))
        return arc_data.get("narrative", "")
    return "No project arc available. This is the first run without bootstrap data."


def load_cumulative_learnings() -> CumulativeLearnings:
    """Load the cumulative learnings file."""
    if CUMULATIVE_LEARNINGS_PATH.exists():
        data = json.loads(CUMULATIVE_LEARNINGS_PATH.read_text(encoding="utf-8"))
        return CumulativeLearnings(**data)
    return CumulativeLearnings()


def save_cumulative_learnings(learnings: CumulativeLearnings) -> None:
    """Save updated cumulative learnings."""
    CUMULATIVE_LEARNINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CUMULATIVE_LEARNINGS_PATH.write_text(
        json.dumps(learnings.model_dump(), indent=2, default=str),
        encoding="utf-8",
    )


def extract_constraints(
    journal_entries: list[JournalEntry],
    claude: ClaudeClient,
) -> str:
    """Extract practical constraints from journal entries.

    Returns:
        JSON string with extracted constraints.
    """
    journal_text = entries_to_summary_text(journal_entries)
    if not journal_text or journal_text == "No journal entries for this period.":
        return "{}"

    prompt = CONSTRAINT_EXTRACTION_PROMPT.format(journal_text=journal_text)
    response = claude.send_message(
        prompt,
        system_prompt="You are a project management assistant analyzing team journals.",
        max_tokens=2048,
    )
    return response


def run_analysis(
    weekly_data: WeeklyData,
    claude: Optional[ClaudeClient] = None,
) -> AnalysisResult:
    """Run Stage 1 analysis on weekly data.

    Args:
        weekly_data: Parsed weekly data (experiments, journals, goals).
        claude: Claude client instance.

    Returns:
        AnalysisResult with AI-generated analysis.
    """
    claude = claude or ClaudeClient()

    # Load context
    project_arc = load_project_arc()
    cumulative = load_cumulative_learnings()

    # Build experiment data text
    exp_texts = []
    for exp in weekly_data.experiments:
        exp_texts.append(experiment_to_summary_text(exp))
    experiment_data = "\n\n---\n\n".join(exp_texts) if exp_texts else "No experiments this week."

    # Build journal text
    journal_text = entries_to_summary_text(weekly_data.journal_entries)

    # Build cumulative learnings text
    cumulative_text = json.dumps(cumulative.model_dump(), indent=2, default=str)

    # Truncate project arc if too long to fit in context
    if len(project_arc) > 50000:
        project_arc = project_arc[:50000] + "\n\n[... truncated for length ...]"

    prompt = ANALYSIS_PROMPT.format(
        project_arc=project_arc,
        experiment_data=experiment_data,
        cumulative_learnings=cumulative_text,
        journal_entries=journal_text,
    )

    response = claude.send_message(
        prompt,
        system_prompt=SYSTEM_PROMPT_SCIENTIST,
        max_tokens=8192,
    )

    # Try to extract updated learnings JSON from the response
    updated_learnings = _extract_learnings_json(response)
    if updated_learnings:
        updated_learnings.last_updated = datetime.now().isoformat()
        updated_learnings.weeks_analyzed = cumulative.weeks_analyzed + 1

    return AnalysisResult(
        raw_response=response,
        updated_learnings=updated_learnings,
    )


def _extract_learnings_json(response: str) -> Optional[CumulativeLearnings]:
    """Extract the updated cumulative learnings JSON from Claude's response."""
    import re

    # Find JSON block in the response
    json_match = re.search(r"```json\s*\n(.*?)\n```", response, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(1))
            return CumulativeLearnings(**data)
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"Failed to parse learnings JSON: {e}")

    return None
