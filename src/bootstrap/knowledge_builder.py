"""One-time historical data processing to build institutional knowledge.

Processes 4 years of data in half-year chunks (H1 2022 -> H2 2025) to build
a comprehensive knowledge base of the Stampede project history.

Run with: python -m src.bootstrap.knowledge_builder
"""

import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from src.analysis.claude_client import ClaudeClient
from src.config import (
    DRIVE_FOLDER_ID,
    EXPERIMENT_BATCH_SIZE,
    HALF_YEAR_PERIODS,
    KNOWLEDGE_DIR,
)
from src.drive.client import DriveClient, MIME_SPREADSHEET, MIME_DOCUMENT
from src.drive.docs import DocsReader
from src.drive.sheets import SheetsReader
from src.models.data import HalfYearSummary, ProjectArc
from src.parsers.experiment_sheet import parse_experiment_grid, experiment_to_summary_text
from src.parsers.journal import parse_journal_text

logger = logging.getLogger(__name__)

HALF_YEAR_SUMMARY_PROMPT = """You are building a comprehensive history of a TB diagnostics R&D project.

## Previous Half-Year Summaries
{previous_summaries}

## This Half-Year: Experiment Batch Summaries
{experiment_summaries}

## This Half-Year: Journal & Meeting Insights
{journal_insights}

Generate a comprehensive summary for this half-year ({period}, {start_date} to {end_date}) covering:

1. MILESTONES & ACHIEVEMENTS
   - What was accomplished this period
   - Performance benchmarks reached (best LOD, sensitivity, specificity)

2. TECHNICAL EVOLUTION
   - How the device, assays, chemistry, or sequences changed
   - Key decisions made and their rationale (e.g., switching polymerases)
   - New experiment families started

3. KEY FINDINGS
   - Most important scientific discoveries
   - What worked well and what didn't
   - Experiment families that were concluded with results

4. CHALLENGES & FAILURES
   - What was tried and didn't work
   - Recurring problems (hardware, reagent, sample issues)
   - How challenges were resolved (or if they remain open)

5. TEAM & PROCESS
   - Who was working on what
   - Any process improvements or organizational changes

6. STATE AT END OF PERIOD
   - Where things stood at the end of this half-year
   - What the immediate next priorities were

If there is no data for this period, note that and summarize what is known."""

EXPERIMENT_BATCH_PROMPT = """For each experiment in this batch, extract:
- What was being tested and why
- Key results (best Ct values, LOD achieved, pass/fail)
- What was learned (conclusions from the Resume field)
- Experiment family it belongs to (e.g., "LOD testing", "Preheat sequence optimization")

## Experiment Data
{experiment_data}"""

JOURNAL_INSIGHTS_PROMPT = """Read the following journal/meeting entries from a TB diagnostics R&D project.
Identify and summarize:

1. Major technical decisions and their rationale
2. Hardware/firmware milestones
3. Team changes or organizational shifts
4. Problems encountered and how they were resolved
5. Key findings or breakthroughs mentioned

## Journal Entries
{journal_text}"""

PROJECT_ARC_PROMPT = """You have summaries from 4 years of a TB diagnostics R&D project.
Synthesize these into a comprehensive project narrative:

1. PROJECT EVOLUTION: How has the device/test evolved from concept to current state?
2. ASSAY JOURNEY: How did the assay strategy evolve? (IS6110, IS1081, rpoB, Human)
3. CHEMISTRY DECISIONS: DsBio HS vs fTaq - what's the history?
4. SAMPLE TYPES: Evolution from liquid controls to tongue swabs to sputum
5. DEVICE GENERATIONS: V1 -> V2 -> V3 progression
6. PERFORMANCE TRAJECTORY: How has LOD, sensitivity, specificity improved over time?
7. FAILED APPROACHES: What dead ends were explored and abandoned?
8. CURRENT STATE OF THE ART: Where does the project stand today?
9. OPEN QUESTIONS: What fundamental questions remain unanswered?
10. INSTITUTIONAL KNOWLEDGE: Key facts any new team member should know

## Half-Year Summaries
{all_summaries}"""


class KnowledgeBuilder:
    """Builds institutional knowledge from historical Drive data."""

    def __init__(
        self,
        drive_client: Optional[DriveClient] = None,
        claude_client: Optional[ClaudeClient] = None,
    ):
        self._drive = drive_client or DriveClient()
        self._claude = claude_client or ClaudeClient()
        self._sheets_reader = SheetsReader(self._drive.sheets)
        self._docs_reader = DocsReader(self._drive.docs)
        KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    def run(self, folder_id: Optional[str] = None) -> None:
        """Run the full bootstrap process.

        Args:
            folder_id: Google Drive folder ID. Defaults to config.
        """
        folder_id = folder_id or DRIVE_FOLDER_ID
        if not folder_id:
            raise ValueError("No Drive folder ID configured")

        previous_summaries: list[str] = []

        for period, start_str, end_str in HALF_YEAR_PERIODS:
            summary_path = KNOWLEDGE_DIR / f"{period}.json"

            # Check if already processed (resumability)
            if summary_path.exists():
                logger.info(f"Skipping {period} (already processed)")
                existing = json.loads(summary_path.read_text())
                previous_summaries.append(
                    f"### {period}\n{existing.get('raw_summary', '')}"
                )
                continue

            logger.info(f"Processing {period} ({start_str} to {end_str})")
            summary = self._process_half_year(
                folder_id, period, start_str, end_str, previous_summaries
            )

            # Save summary
            summary_path.write_text(
                json.dumps(summary.model_dump(), indent=2, default=str),
                encoding="utf-8",
            )
            previous_summaries.append(f"### {period}\n{summary.raw_summary}")
            logger.info(f"Saved {period} summary")

        # Synthesize project arc
        self._synthesize_project_arc(previous_summaries)
        logger.info("Bootstrap complete!")

    def _process_half_year(
        self,
        folder_id: str,
        period: str,
        start_str: str,
        end_str: str,
        previous_summaries: list[str],
    ) -> HalfYearSummary:
        """Process a single half-year period."""
        start_date = date.fromisoformat(start_str)
        end_date = date.fromisoformat(end_str)

        # Discover all files in the folder
        all_spreadsheets = self._drive.list_spreadsheets(folder_id)
        all_documents = self._drive.list_documents(folder_id)

        # Filter files by date (using filename date patterns or modified time)
        period_sheets = self._filter_by_period(all_spreadsheets, start_date, end_date)
        period_docs = self._filter_by_period(all_documents, start_date, end_date)

        logger.info(
            f"  Found {len(period_sheets)} sheets, {len(period_docs)} docs for {period}"
        )

        # Parse and batch-analyze experiment sheets
        experiment_summaries = self._process_experiment_sheets(period_sheets)

        # Parse and analyze journals/docs
        journal_insights = self._process_documents(period_docs)

        # Generate half-year summary
        prev_text = "\n\n".join(previous_summaries) if previous_summaries else "None (this is the first period)"

        prompt = HALF_YEAR_SUMMARY_PROMPT.format(
            previous_summaries=prev_text,
            experiment_summaries=experiment_summaries or "No experiment data found for this period.",
            journal_insights=journal_insights or "No journal data found for this period.",
            period=period,
            start_date=start_str,
            end_date=end_str,
        )

        raw_summary = self._claude.send_message(prompt, max_tokens=4096)

        return HalfYearSummary(
            period=period,
            start_date=start_str,
            end_date=end_str,
            raw_summary=raw_summary,
            experiments_processed=len(period_sheets),
            journals_processed=len(period_docs),
        )

    def _filter_by_period(
        self, files: list[dict], start_date: date, end_date: date
    ) -> list[dict]:
        """Filter files to those belonging to a specific half-year period."""
        filtered = []
        for f in files:
            # Try to match by modified time
            mod_time_str = f.get("modifiedTime", "")
            if mod_time_str:
                try:
                    mod_date = datetime.fromisoformat(
                        mod_time_str.replace("Z", "+00:00")
                    ).date()
                    if start_date <= mod_date <= end_date:
                        filtered.append(f)
                        continue
                except ValueError:
                    pass

            # Try to match by filename date patterns (e.g., "H1 2022", "01_05_2026")
            name = f.get("name", "")
            if self._filename_matches_period(name, start_date, end_date):
                filtered.append(f)

        return filtered

    def _filename_matches_period(
        self, name: str, start_date: date, end_date: date
    ) -> bool:
        """Check if a filename contains date info matching the period."""
        import re

        # Check for "H1 YYYY" or "H2 YYYY" pattern
        m = re.search(r"H([12])\s*(\d{4})", name)
        if m:
            half = int(m.group(1))
            year = int(m.group(2))
            if half == 1:
                file_start = date(year, 1, 1)
                file_end = date(year, 6, 30)
            else:
                file_start = date(year, 7, 1)
                file_end = date(year, 12, 31)
            return (
                start_date <= file_start <= end_date
                or start_date <= file_end <= end_date
            )

        # Check for MM_DD_YYYY pattern
        m = re.search(r"(\d{2})_(\d{2})_(\d{4})", name)
        if m:
            try:
                file_date = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
                return start_date <= file_date <= end_date
            except ValueError:
                pass

        return False

    def _process_experiment_sheets(self, sheets: list[dict]) -> str:
        """Parse and batch-analyze experiment sheets."""
        if not sheets:
            return ""

        # Parse all sheets
        experiments_text = []
        for sheet_file in sheets:
            try:
                grid = self._sheets_reader.read_sheet(sheet_file["id"])
                exp = parse_experiment_grid(grid, sheet_file.get("name", ""))
                text = experiment_to_summary_text(exp)
                experiments_text.append(text)
            except Exception as e:
                logger.warning(f"Failed to parse sheet {sheet_file.get('name')}: {e}")

        if not experiments_text:
            return ""

        # Batch experiments and send to Claude for analysis
        batch_summaries = []
        for i in range(0, len(experiments_text), EXPERIMENT_BATCH_SIZE):
            batch = experiments_text[i : i + EXPERIMENT_BATCH_SIZE]
            batch_data = "\n\n---\n\n".join(batch)

            prompt = EXPERIMENT_BATCH_PROMPT.format(experiment_data=batch_data)

            try:
                summary = self._claude.send_message(prompt, max_tokens=4096)
                batch_summaries.append(summary)
            except Exception as e:
                logger.error(f"Failed to analyze experiment batch: {e}")

        return "\n\n---\n\n".join(batch_summaries)

    def _process_documents(self, docs: list[dict]) -> str:
        """Parse and analyze journal/meeting documents."""
        if not docs:
            return ""

        all_text_parts = []
        for doc_file in docs:
            try:
                text = self._docs_reader.read_document_text(doc_file["id"])
                all_text_parts.append(
                    f"### {doc_file.get('name', 'Unknown Document')}\n{text}"
                )
            except Exception as e:
                logger.warning(f"Failed to read doc {doc_file.get('name')}: {e}")

        if not all_text_parts:
            return ""

        combined_text = "\n\n---\n\n".join(all_text_parts)

        # If text is very long, truncate to fit token limits
        if len(combined_text) > 100000:
            combined_text = combined_text[:100000] + "\n\n[... truncated ...]"

        prompt = JOURNAL_INSIGHTS_PROMPT.format(journal_text=combined_text)

        try:
            return self._claude.send_message(prompt, max_tokens=4096)
        except Exception as e:
            logger.error(f"Failed to analyze documents: {e}")
            return ""

    def _synthesize_project_arc(self, all_summaries: list[str]) -> None:
        """Generate the overall project arc from all half-year summaries."""
        arc_path = KNOWLEDGE_DIR / "project_arc.json"

        if not all_summaries:
            logger.warning("No summaries to synthesize into project arc")
            return

        combined = "\n\n".join(all_summaries)
        prompt = PROJECT_ARC_PROMPT.format(all_summaries=combined)

        try:
            narrative = self._claude.send_message(prompt, max_tokens=8192)
        except Exception as e:
            logger.error(f"Failed to synthesize project arc: {e}")
            narrative = "Failed to generate project arc."

        arc = ProjectArc(
            generated_date=datetime.now().isoformat(),
            narrative=narrative,
            half_year_summaries=[p[0] for p in HALF_YEAR_PERIODS],
        )

        arc_path.write_text(
            json.dumps(arc.model_dump(), indent=2), encoding="utf-8"
        )
        logger.info("Saved project_arc.json")


def process_single_half(
    half: str,
    folder_id: str,
    output_dir: Optional[Path] = None,
) -> Path:
    """Process a single half-year period and save draft for review.

    Args:
        half: Period name (e.g., "H1_2022")
        folder_id: Google Drive folder ID containing the data
        output_dir: Output directory (defaults to KNOWLEDGE_DIR)

    Returns:
        Path to the generated draft markdown file.
    """
    output_dir = output_dir or KNOWLEDGE_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find the period config
    period_config = None
    for period, start_str, end_str in HALF_YEAR_PERIODS:
        if period == half:
            period_config = (period, start_str, end_str)
            break

    if not period_config:
        raise ValueError(f"Unknown half-year period: {half}. Valid options: {[p[0] for p in HALF_YEAR_PERIODS]}")

    period, start_str, end_str = period_config

    logger.info(f"=== Processing {period} ({start_str} to {end_str}) ===")
    logger.info(f"Folder ID: {folder_id}")

    # Initialize clients
    drive = DriveClient()
    claude = ClaudeClient()
    sheets_reader = SheetsReader(drive.sheets)
    docs_reader = DocsReader(drive.docs)

    # Load any previous summaries for context
    previous_summaries = []
    for prev_period, _, _ in HALF_YEAR_PERIODS:
        if prev_period == period:
            break
        prev_path = output_dir / f"{prev_period}.md"
        if prev_path.exists():
            prev_content = prev_path.read_text(encoding="utf-8")
            previous_summaries.append(f"### {prev_period}\n{prev_content}")
            logger.info(f"  Loaded previous summary: {prev_period}")

    # Discover files in the folder (all files, not filtered by date)
    logger.info("Discovering files in folder...")
    all_spreadsheets = drive.list_files_in_folder(folder_id, mime_type=MIME_SPREADSHEET)
    all_documents = drive.list_files_in_folder(folder_id, mime_type=MIME_DOCUMENT)
    logger.info(f"  Found {len(all_spreadsheets)} spreadsheets")
    logger.info(f"  Found {len(all_documents)} documents")

    # Parse experiment sheets
    logger.info("Parsing experiment sheets...")
    experiments_text = []
    for i, sheet_file in enumerate(all_spreadsheets):
        name = sheet_file.get("name", "")
        if "goal" in name.lower():
            continue
        try:
            grid = sheets_reader.read_sheet(sheet_file["id"])
            exp = parse_experiment_grid(grid, name)
            text = experiment_to_summary_text(exp)
            experiments_text.append(text)
            logger.info(f"  [{i+1}/{len(all_spreadsheets)}] Parsed: {name}")
        except Exception as e:
            logger.warning(f"  [{i+1}/{len(all_spreadsheets)}] Failed: {name} - {e}")

    logger.info(f"  Successfully parsed {len(experiments_text)} experiments")

    # Batch-analyze experiments with Claude
    logger.info("Analyzing experiments with Claude...")
    batch_summaries = []
    batch_size = EXPERIMENT_BATCH_SIZE
    for i in range(0, len(experiments_text), batch_size):
        batch = experiments_text[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(experiments_text) + batch_size - 1) // batch_size
        logger.info(f"  Processing batch {batch_num}/{total_batches} ({len(batch)} experiments)")

        batch_data = "\n\n---\n\n".join(batch)
        prompt = EXPERIMENT_BATCH_PROMPT.format(experiment_data=batch_data)

        try:
            summary = claude.send_message(prompt, max_tokens=4096)
            batch_summaries.append(summary)
        except Exception as e:
            logger.error(f"  Batch {batch_num} failed: {e}")

    experiment_analysis = "\n\n---\n\n".join(batch_summaries) if batch_summaries else "No experiment data found."

    # Parse and analyze documents
    logger.info("Parsing and analyzing documents...")
    doc_texts = []
    for i, doc_file in enumerate(all_documents):
        name = doc_file.get("name", "")
        try:
            text = docs_reader.read_document_text(doc_file["id"])
            doc_texts.append(f"### {name}\n{text}")
            logger.info(f"  [{i+1}/{len(all_documents)}] Read: {name}")
        except Exception as e:
            logger.warning(f"  [{i+1}/{len(all_documents)}] Failed: {name} - {e}")

    if doc_texts:
        combined_docs = "\n\n---\n\n".join(doc_texts)
        if len(combined_docs) > 100000:
            combined_docs = combined_docs[:100000] + "\n\n[... truncated ...]"

        prompt = JOURNAL_INSIGHTS_PROMPT.format(journal_text=combined_docs)
        try:
            journal_analysis = claude.send_message(prompt, max_tokens=4096)
        except Exception as e:
            logger.error(f"Journal analysis failed: {e}")
            journal_analysis = "Failed to analyze documents."
    else:
        journal_analysis = "No documents found."

    # Generate half-year summary
    logger.info("Generating half-year summary...")
    prev_text = "\n\n".join(previous_summaries) if previous_summaries else "None (this is the first period)"

    prompt = HALF_YEAR_SUMMARY_PROMPT.format(
        previous_summaries=prev_text,
        experiment_summaries=experiment_analysis,
        journal_insights=journal_analysis,
        period=period,
        start_date=start_str,
        end_date=end_str,
    )

    try:
        summary = claude.send_message(prompt, max_tokens=4096)
    except Exception as e:
        logger.error(f"Summary generation failed: {e}")
        summary = f"Failed to generate summary: {e}"

    # Save as draft markdown for review
    draft_path = output_dir / f"{period}_DRAFT.md"
    draft_content = f"""# {period} Summary (DRAFT)

**Period**: {start_str} to {end_str}
**Spreadsheets processed**: {len(experiments_text)}
**Documents processed**: {len(doc_texts)}
**Generated**: {datetime.now().isoformat()}

---

## Summary

{summary}

---

## Raw Experiment Analysis

{experiment_analysis}

---

## Raw Journal Analysis

{journal_analysis}

---

**REVIEW INSTRUCTIONS**:
1. Review the summary above for accuracy
2. Edit as needed
3. When satisfied, rename this file from `{period}_DRAFT.md` to `{period}.md`
4. Commit and push, then proceed to the next half-year period
"""

    draft_path.write_text(draft_content, encoding="utf-8")
    logger.info(f"Saved draft to: {draft_path}")

    return draft_path


def main():
    """Entry point for bootstrap."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Parse arguments
    half = None
    folder_id = None

    for arg in sys.argv[1:]:
        if arg.startswith("--half="):
            half = arg.split("=")[1]
        elif arg.startswith("--folder="):
            folder_id = arg.split("=")[1]

    if half and folder_id:
        # Single half-year mode
        draft_path = process_single_half(half, folder_id)
        print(f"\n{'='*60}")
        print(f"DRAFT SAVED: {draft_path}")
        print(f"{'='*60}")
        print("\nNext steps:")
        print("1. Review the draft file for accuracy")
        print("2. Edit as needed")
        print(f"3. Rename from {half}_DRAFT.md to {half}.md")
        print("4. Commit and push")
        print("5. Run the next half-year period")
    elif half or folder_id:
        print("Error: Both --half and --folder are required for single-half mode")
        print("Usage: python -m src.bootstrap.knowledge_builder --half=H1_2022 --folder=FOLDER_ID")
        print("\nAvailable periods:")
        for period, start, end in HALF_YEAR_PERIODS:
            print(f"  {period}: {start} to {end}")
        sys.exit(1)
    else:
        # Full bootstrap mode (legacy)
        builder = KnowledgeBuilder()
        builder.run()


if __name__ == "__main__":
    main()
