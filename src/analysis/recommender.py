"""Experiment recommendation engine using Claude API.

Stage 2: Generate experiment recommendations based on analysis results,
goals, and practical constraints.
"""

import logging
from typing import Optional

from src.analysis.claude_client import ClaudeClient, SYSTEM_PROMPT_SCIENTIST
from src.models.data import (
    AnalysisResult,
    Goal,
    RecommendationResult,
)
from src.parsers.goals import goals_to_summary_text

logger = logging.getLogger(__name__)

RECOMMENDATION_PROMPT = """## Team Goals (with deadlines and requirements)
{goals_text}

## Analysis Summary (from Stage 1)
{analysis_summary}

## Practical Constraints (auto-extracted from journals)
{constraints}

## Your Recommendation Tasks:

1. GOAL URGENCY ASSESSMENT
   - For each goal, assess: days remaining, estimated % complete, risk level
   - Flag any goals at risk of being missed

2. STRATEGIC DIRECTION (for PM)
   - What should the team focus on this week and why?
   - Are there any pivots needed based on recent results?
   - Explicitly connect your strategy to specific goals and deadlines

3. SPECIFIC EXPERIMENT RECOMMENDATIONS (3-5 experiments)
   For each recommended experiment:
   - **Title**: Descriptive experiment name
   - **Rationale**: Why this experiment, what question does it answer
   - **Goal alignment**: Which goal(s) it advances and how
   - **Parameters**:
     - Assay(s): which assays to use
     - Polymerase: DsBio HS or fTaq (and why)
     - Sample type: tongue swab, sputum, liquid control, etc.
     - Concentrations: specific copy numbers to test
     - Sequence: which thermocycling protocol (e.g., V6 preheat+touchdown)
     - Device: which device to use (considering availability)
   - **Assigned to**: Suggested scientist (considering availability and expertise)
   - **Expected outcome**: What result would be a success?
   - **Decision criteria**: What will we learn, and what decision does it inform?
   - **Priority**: High / Medium / Low

4. EXPERIMENTS TO AVOID
   - Any experiments that would be redundant given existing data
   - Any experiments that are premature (need prerequisite results first)"""


def run_recommendations(
    analysis_result: AnalysisResult,
    goals: list[Goal],
    constraints_json: str,
    claude: Optional[ClaudeClient] = None,
) -> RecommendationResult:
    """Run Stage 2 recommendation generation.

    Args:
        analysis_result: Output from Stage 1 analysis.
        goals: Parsed team goals.
        constraints_json: Auto-extracted constraints JSON string.
        claude: Claude client instance.

    Returns:
        RecommendationResult with AI-generated recommendations.
    """
    claude = claude or ClaudeClient()

    goals_text = goals_to_summary_text(goals)

    prompt = RECOMMENDATION_PROMPT.format(
        goals_text=goals_text,
        analysis_summary=analysis_result.raw_response,
        constraints=constraints_json,
    )

    response = claude.send_message(
        prompt,
        system_prompt=SYSTEM_PROMPT_SCIENTIST,
        max_tokens=8192,
    )

    return RecommendationResult(raw_response=response)
