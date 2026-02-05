"""Main orchestrator for the Stampede Weekly Report pipeline.

Wires together all pipeline stages:
1. Ingest data from Google Drive (experiments, journals, goals)
2. Parse data using specialized parsers
3. Run AI analysis (Stage 1: summarize, Stage 2: recommend)
4. Generate charts
5. Build Google Slides report

Run with: python -m src.main
"""

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from src.analysis.claude_client import ClaudeClient
from src.analysis.recommender import run_recommendations
from src.analysis.summarizer import (
    extract_constraints,
    load_cumulative_learnings,
    run_analysis,
    save_cumulative_learnings,
)
from src.config import (
    CHARTS_DIR,
    DRIVE_FOLDER_ID,
    REPORTS_FOLDER_ID,
)
from src.drive.client import DriveClient, MIME_DOCUMENT, MIME_SPREADSHEET
from src.drive.docs import DocsReader
from src.drive.sheets import SheetsReader
from src.graphics.charts import generate_all_charts
from src.models.data import Experiment, Goal, JournalEntry, WeeklyData
from src.output.slides import create_weekly_report
from src.parsers.experiment_sheet import parse_experiment_grid
from src.parsers.goals import parse_goals_grid
from src.parsers.journal import parse_journal_text

logger = logging.getLogger(__name__)


def run_weekly_pipeline(
    days_back: int = 7,
    folder_id: Optional[str] = None,
    reports_folder_id: Optional[str] = None,
    dry_run: bool = False,
    all_files: bool = False,
) -> Optional[str]:
    """Run the full weekly report pipeline.

    Args:
        days_back: Number of days to look back for data.
        folder_id: Google Drive folder ID (defaults to config).
        reports_folder_id: Output folder ID (defaults to config).
        dry_run: If True, skip Slides generation and just print analysis.
        all_files: If True, process all files in folder (for historical data).

    Returns:
        URL of the generated Slides report, or None if dry_run.
    """
    folder_id = folder_id or DRIVE_FOLDER_ID
    reports_folder_id = reports_folder_id or REPORTS_FOLDER_ID

    if not folder_id:
        raise ValueError("DRIVE_FOLDER_ID not set")

    logger.info("=== Stampede Weekly Report Pipeline ===")

    # Initialize clients
    drive = DriveClient()
    claude = ClaudeClient()
    sheets_reader = SheetsReader(drive.sheets)
    docs_reader = DocsReader(drive.docs)

    # Calculate date range
    today = date.today()
    week_end = today
    week_start = today - timedelta(days=days_back)

    if all_files:
        logger.info(f"Processing ALL files in folder: {folder_id}")
    else:
        logger.info(f"Analyzing period: {week_start} to {week_end}")

    # === Stage 0: Data Ingestion ===
    logger.info("Stage 0: Ingesting data from Google Drive...")

    # Discover experiment sheets (all files or recent only)
    if all_files:
        recent_sheets = drive.list_files_in_folder(folder_id, mime_type=MIME_SPREADSHEET)
        logger.info(f"  Found {len(recent_sheets)} spreadsheets")
        recent_docs = drive.list_files_in_folder(folder_id, mime_type=MIME_DOCUMENT)
        logger.info(f"  Found {len(recent_docs)} documents")
    else:
        recent_sheets = drive.list_recent_files(folder_id, days=days_back, mime_type=MIME_SPREADSHEET)
        logger.info(f"  Found {len(recent_sheets)} recent spreadsheets")
        recent_docs = drive.list_recent_files(folder_id, days=days_back, mime_type=MIME_DOCUMENT)
        logger.info(f"  Found {len(recent_docs)} recent documents")

    # Parse experiment sheets
    experiments: list[Experiment] = []
    for sheet_file in recent_sheets:
        name = sheet_file.get("name", "")
        # Skip non-experiment files (e.g., goals, journals)
        if "goal" in name.lower() or "journal" in name.lower():
            continue
        try:
            grid = sheets_reader.read_sheet(sheet_file["id"])
            exp = parse_experiment_grid(grid, name)
            if exp.runs:  # Only include sheets that have experiment data
                experiments.append(exp)
        except Exception as e:
            logger.warning(f"  Failed to parse sheet '{name}': {e}")

    logger.info(f"  Parsed {len(experiments)} experiments with data")

    # Parse journal documents
    journal_entries: list[JournalEntry] = []
    for doc_file in recent_docs:
        name = doc_file.get("name", "")
        try:
            text = docs_reader.read_document_text(doc_file["id"])
            entries = parse_journal_text(text, name)
            if all_files:
                # For historical data, include all entries
                journal_entries.extend(entries)
            else:
                # Filter to this week's entries
                week_entries = [
                    e for e in entries
                    if e.entry_date is not None and week_start <= e.entry_date <= week_end
                ]
                journal_entries.extend(week_entries)
        except Exception as e:
            logger.warning(f"  Failed to parse doc '{name}': {e}")

    logger.info(f"  Found {len(journal_entries)} journal entries for this week")

    # Parse goals (look for goals spreadsheet)
    goals: list[Goal] = []
    all_sheets = drive.list_spreadsheets(folder_id)
    for sheet_file in all_sheets:
        if "goal" in sheet_file.get("name", "").lower():
            try:
                grid = sheets_reader.read_sheet(sheet_file["id"])
                goals = parse_goals_grid(grid, sheet_file.get("name", ""))
                logger.info(f"  Parsed {len(goals)} goals from '{sheet_file['name']}'")
            except Exception as e:
                logger.warning(f"  Failed to parse goals sheet: {e}")
            break

    # Build weekly data model
    weekly_data = WeeklyData(
        week_start=week_start,
        week_end=week_end,
        experiments=experiments,
        journal_entries=journal_entries,
        goals=goals,
    )

    # === Pre-Stage: Extract Constraints ===
    logger.info("Extracting practical constraints from journals...")
    constraints_json = extract_constraints(journal_entries, claude)
    logger.info("  Constraints extracted")

    # === Stage 1: Analysis ===
    logger.info("Stage 1: Running AI analysis...")
    analysis = run_analysis(weekly_data, claude)
    logger.info("  Analysis complete")

    # === Stage 2: Recommendations ===
    logger.info("Stage 2: Generating recommendations...")
    recommendations = run_recommendations(analysis, goals, constraints_json, claude)
    logger.info("  Recommendations generated")

    # === Stage 3: Update Cumulative Learnings ===
    if analysis.updated_learnings:
        save_cumulative_learnings(analysis.updated_learnings)
        logger.info("  Updated cumulative_learnings.json")

    if dry_run:
        logger.info("\n=== DRY RUN - Analysis Output ===")
        print("\n--- ANALYSIS ---")
        print(analysis.raw_response)
        print("\n--- RECOMMENDATIONS ---")
        print(recommendations.raw_response)
        print("\n--- CONSTRAINTS ---")
        print(constraints_json)
        return None

    # === Stage 4: Generate Charts ===
    logger.info("Stage 4: Generating charts...")
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    chart_paths = generate_all_charts(
        experiments, goals, recommendations.raw_response, CHARTS_DIR
    )
    logger.info(f"  Generated {len(chart_paths)} charts")

    # === Stage 5: Build Slides Report ===
    logger.info("Stage 5: Building Google Slides report...")
    report_url = create_weekly_report(
        drive_client=drive,
        week_start=week_start,
        week_end=week_end,
        analysis=analysis,
        recommendations=recommendations,
        experiments=experiments,
        chart_paths=chart_paths,
        reports_folder_id=reports_folder_id,
    )
    logger.info(f"  Report created: {report_url}")

    return report_url


def main():
    """Entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Parse command line arguments
    dry_run = "--dry-run" in sys.argv
    all_files = "--all" in sys.argv  # Process all files, not just recent

    if dry_run:
        logger.info("Running in dry-run mode (no Slides output)")

    # Check for custom days-back
    days_back = 7
    folder_id = None

    for arg in sys.argv[1:]:
        if arg.startswith("--days="):
            days_back = int(arg.split("=")[1])
        elif arg.startswith("--folder="):
            folder_id = arg.split("=")[1]

    url = run_weekly_pipeline(
        days_back=days_back,
        folder_id=folder_id,
        dry_run=dry_run,
        all_files=all_files,
    )
    if url:
        print(f"\nReport URL: {url}")


if __name__ == "__main__":
    main()
