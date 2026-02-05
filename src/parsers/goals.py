"""Parser for the Stampede Goals CSV spreadsheet."""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

from src.models.data import Goal

logger = logging.getLogger(__name__)


def parse_goals_csv(filepath: str | Path) -> list[Goal]:
    """Parse the goals CSV file into a list of Goal objects.

    The goals CSV has a specific structure:
    - Row 4: Column headers (Active goal (short), ..., Active goal -reqs, Team Points, ...)
    - Row 5+: Section header (e.g., "Stampede / Discoplex")
    - Subsequent rows: Individual goals, some spanning multiple rows for requirements

    Args:
        filepath: Path to the goals CSV file.

    Returns:
        List of parsed Goal objects.
    """
    filepath = Path(filepath)
    rows = _read_csv(filepath)
    if not rows:
        return []

    goals = []
    i = 0

    while i < len(rows):
        row = rows[i]

        # Look for goal rows: col B (index 1) has the short name
        short_name = _cell(row, 1).strip()
        if not short_name or short_name.lower() in (
            "high", "low", "", "individual % check:",
        ):
            i += 1
            continue

        # Skip header rows and section headers
        if short_name.lower().startswith("active goal") or "/" in short_name:
            i += 1
            continue

        # This looks like a goal row. Extract requirements from col C (index 3)
        requirements_lines = [_cell(row, 3)]
        points_str = _cell(row, 4)
        sign_off = _cell(row, 5)
        due_date = _cell(row, 6)
        goal_type = _cell(row, 7)

        # Requirements may span multiple rows (multiline cell in CSV)
        j = i + 1
        while j < len(rows):
            next_short = _cell(rows[j], 1).strip()
            # If next row has a new short name or is a known marker, stop
            if next_short and next_short.lower() not in ("",):
                break
            # Check if this row has requirement continuation text in col 3
            req_text = _cell(rows[j], 3)
            if req_text:
                requirements_lines.append(req_text)
            # Also check if points are in this row (sometimes split across rows)
            if not points_str:
                points_str = _cell(rows[j], 4)
            if not due_date:
                due_date = _cell(rows[j], 6)
            j += 1

        requirements = "\n".join(line for line in requirements_lines if line)

        # Parse points
        try:
            points = int(points_str) if points_str else 0
        except ValueError:
            points = 0

        # Skip non-goal rows (like "Total" or formatting rows)
        if short_name.lower() in ("total", "stampede / discoplex", "stampede"):
            i = j
            continue

        goal = Goal(
            short_name=short_name,
            requirements=requirements,
            points=points,
            sign_off=sign_off,
            due_date=due_date,
            goal_type=goal_type,
        )
        goals.append(goal)
        i = j

    return goals


def parse_goals_grid(rows: list[list[str]], source_name: str = "") -> list[Goal]:
    """Parse goals from a pre-loaded grid of cell values (Google Sheets API)."""
    # Same logic but operating on pre-loaded grid
    goals = []
    i = 0

    while i < len(rows):
        row = rows[i] if i < len(rows) else []
        short_name = _cell(row, 1).strip()

        if not short_name or short_name.lower() in (
            "high", "low", "individual % check:",
        ):
            i += 1
            continue

        if short_name.lower().startswith("active goal") or "/" in short_name:
            i += 1
            continue

        requirements_lines = [_cell(row, 3)]
        points_str = _cell(row, 4)
        sign_off = _cell(row, 5)
        due_date = _cell(row, 6)
        goal_type = _cell(row, 7)

        j = i + 1
        while j < len(rows):
            next_short = _cell(rows[j], 1).strip()
            if next_short:
                break
            req_text = _cell(rows[j], 3)
            if req_text:
                requirements_lines.append(req_text)
            if not points_str:
                points_str = _cell(rows[j], 4)
            if not due_date:
                due_date = _cell(rows[j], 6)
            j += 1

        requirements = "\n".join(line for line in requirements_lines if line)

        try:
            points = int(points_str) if points_str else 0
        except ValueError:
            points = 0

        if short_name.lower() in ("total", "stampede / discoplex", "stampede"):
            i = j
            continue

        goals.append(Goal(
            short_name=short_name,
            requirements=requirements,
            points=points,
            sign_off=sign_off,
            due_date=due_date,
            goal_type=goal_type,
        ))
        i = j

    return goals


def goals_to_summary_text(goals: list[Goal]) -> str:
    """Convert goals to a text summary for AI analysis."""
    lines = ["## Team Goals\n"]
    for g in goals:
        lines.append(f"### {g.short_name} ({g.points} pts)")
        lines.append(f"**Due**: {g.due_date}")
        lines.append(f"**Requirements**: {g.requirements}")
        if g.notes:
            lines.append(f"**Notes**: {g.notes}")
        lines.append("")
    return "\n".join(lines)


def _read_csv(filepath: Path) -> list[list[str]]:
    rows = []
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        logger.error(f"Failed to read goals CSV {filepath}: {e}")
    return rows


def _cell(row: list[str], idx: int) -> str:
    if idx < 0 or idx >= len(row):
        return ""
    return row[idx].strip()
