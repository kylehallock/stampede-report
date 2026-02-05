"""Parser for journal/meeting minute text files and docx files.

Handles multiple date formats and splits content by date boundaries.
"""

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from src.models.data import JournalEntry

logger = logging.getLogger(__name__)

# Date patterns to detect entry boundaries
DATE_PATTERNS = [
    # MM/DD/YYYY
    (re.compile(r"^(\d{1,2}/\d{1,2}/\d{4})\s*$"), "%m/%d/%Y"),
    # YYYY-MM-DD
    (re.compile(r"^(\d{4}-\d{2}-\d{2})\s*$"), "%Y-%m-%d"),
    # Month DD, YYYY
    (re.compile(r"^((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})\s*$", re.IGNORECASE), "%B %d, %Y"),
    # MM-DD-YYYY
    (re.compile(r"^(\d{1,2}-\d{1,2}-\d{4})\s*$"), "%m-%d-%Y"),
]

# Image/comment placeholder pattern to strip
PLACEHOLDER_PATTERN = re.compile(r"\[([a-z])\]")


def parse_journal_txt(filepath: str | Path) -> list[JournalEntry]:
    """Parse a plain text journal file into dated entries.

    Args:
        filepath: Path to the .txt journal file.

    Returns:
        List of JournalEntry objects, sorted by date descending.
    """
    filepath = Path(filepath)
    try:
        text = filepath.read_text(encoding="utf-8-sig")
    except Exception as e:
        logger.error(f"Failed to read journal {filepath}: {e}")
        return []

    return _parse_journal_text(text, filepath.name)


def parse_journal_docx(filepath: str | Path) -> list[JournalEntry]:
    """Parse a .docx journal file into dated entries.

    Args:
        filepath: Path to the .docx file.

    Returns:
        List of JournalEntry objects.
    """
    filepath = Path(filepath)
    try:
        import docx
        doc = docx.Document(str(filepath))
        text = "\n".join(para.text for para in doc.paragraphs)
    except ImportError:
        logger.warning("python-docx not installed, skipping .docx file")
        return []
    except Exception as e:
        logger.error(f"Failed to read docx {filepath}: {e}")
        return []

    return _parse_journal_text(text, filepath.name)


def parse_journal_text(text: str, source_name: str = "") -> list[JournalEntry]:
    """Parse journal text content directly (e.g., from Google Docs API).

    Args:
        text: The journal text content.
        source_name: Name/ID of the source document.

    Returns:
        List of JournalEntry objects.
    """
    return _parse_journal_text(text, source_name)


def _parse_journal_text(text: str, source_name: str) -> list[JournalEntry]:
    """Core parsing logic for journal text content."""
    lines = text.split("\n")
    entries = []
    current_date: Optional[date] = None
    current_date_str = ""
    current_author = ""
    current_content_lines: list[str] = []
    collecting_content = False

    for line_raw in lines:
        line = line_raw.rstrip()

        # Check if this line is a date boundary
        parsed_date = _try_parse_date(line.strip())
        if parsed_date is not None:
            # Save previous entry if we have content
            if current_date is not None and (current_content_lines or current_author):
                entries.append(_build_entry(
                    current_date, current_date_str, current_author,
                    current_content_lines, source_name
                ))

            current_date = parsed_date
            current_date_str = line.strip()
            current_author = ""
            current_content_lines = []
            collecting_content = False
            continue

        # If we have a date but no author yet, the next non-empty line is the author
        if current_date is not None and not current_author and not collecting_content:
            stripped = line.strip()
            if stripped:
                # Check if this looks like an author name (short, no special chars)
                if len(stripped) < 40 and not stripped.startswith("*") and not stripped.startswith("-"):
                    current_author = stripped
                    collecting_content = True
                    continue
                else:
                    # Content starts immediately (no separate author line)
                    current_author = ""
                    collecting_content = True
                    current_content_lines.append(line)
            continue

        # Check if this is a new author within the same date
        # (some journals have multiple authors per date, separated by just a name on its own line)
        if current_date is not None and collecting_content:
            stripped = line.strip()
            if (
                stripped
                and len(stripped) < 30
                and not stripped.startswith("*")
                and not stripped.startswith("-")
                and not stripped.startswith("#")
                and not any(c.isdigit() for c in stripped)
                and stripped.istitle()
                and " " not in stripped.strip()  # single word, capitalized
                and len(current_content_lines) > 0
                and not current_content_lines[-1].strip()  # preceded by blank line
            ):
                # This might be a new author block under the same date
                # Save current entry and start new one
                if current_content_lines:
                    entries.append(_build_entry(
                        current_date, current_date_str, current_author,
                        current_content_lines, source_name
                    ))
                current_author = stripped
                current_content_lines = []
                continue

        # Regular content line
        if current_date is not None:
            collecting_content = True
            current_content_lines.append(line)

    # Don't forget the last entry
    if current_date is not None and (current_content_lines or current_author):
        entries.append(_build_entry(
            current_date, current_date_str, current_author,
            current_content_lines, source_name
        ))

    return entries


def _try_parse_date(text: str) -> Optional[date]:
    """Try to parse a line as a date using all known patterns."""
    for pattern, fmt in DATE_PATTERNS:
        m = pattern.match(text)
        if m:
            date_str = m.group(1)
            # Handle comma-optional format
            date_str = date_str.replace(",", "")
            try:
                return datetime.strptime(date_str, fmt.replace(",", "")).date()
            except ValueError:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
    return None


def _build_entry(
    entry_date: date,
    date_str: str,
    author: str,
    content_lines: list[str],
    source_name: str,
) -> JournalEntry:
    """Build a JournalEntry from accumulated data."""
    # Clean content: strip image placeholders and excessive whitespace
    content = "\n".join(content_lines)
    content = PLACEHOLDER_PATTERN.sub("", content)
    # Collapse multiple blank lines
    content = re.sub(r"\n{3,}", "\n\n", content)
    content = content.strip()

    return JournalEntry(
        entry_date=entry_date,
        date_str=date_str,
        author=author,
        content=content,
        source_file=source_name,
    )


def filter_entries_by_date_range(
    entries: list[JournalEntry],
    start_date: date,
    end_date: date,
) -> list[JournalEntry]:
    """Filter journal entries to a specific date range."""
    return [
        e for e in entries
        if e.entry_date is not None and start_date <= e.entry_date <= end_date
    ]


def entries_to_summary_text(entries: list[JournalEntry]) -> str:
    """Convert journal entries to a text summary for AI analysis."""
    if not entries:
        return "No journal entries for this period."

    lines = ["## Journal Entries\n"]
    # Group by date
    by_date: dict[str, list[JournalEntry]] = {}
    for e in sorted(entries, key=lambda x: x.entry_date or date.min, reverse=True):
        key = e.date_str or str(e.entry_date)
        by_date.setdefault(key, []).append(e)

    for date_key, date_entries in by_date.items():
        lines.append(f"### {date_key}")
        for e in date_entries:
            if e.author:
                lines.append(f"**{e.author}**")
            lines.append(e.content)
            lines.append("")

    return "\n".join(lines)
