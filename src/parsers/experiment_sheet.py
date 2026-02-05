"""Parser for semi-structured experiment device testing CSV sheets.

Handles the variability across experiment sheet formats by scanning for
landmark cells to detect section boundaries dynamically.
"""

from __future__ import annotations

import csv
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from src.models.data import (
    ChannelAssignment,
    CtValues,
    Experiment,
    ReagentFormulation,
    ReagentItem,
    Run,
    SequenceSetup,
    SequenceStep,
)

logger = logging.getLogger(__name__)


def parse_experiment_csv(filepath: str | Path) -> Experiment:
    """Parse an experiment CSV file into an Experiment model.

    Args:
        filepath: Path to the CSV file.

    Returns:
        Parsed Experiment object.
    """
    filepath = Path(filepath)
    rows = _read_csv(filepath)
    if not rows:
        return Experiment(source_file=filepath.name)

    exp = Experiment(source_file=filepath.name)

    # Extract date from filename
    exp.experiment_date = _extract_date_from_filename(filepath.name)

    # Parse header metadata
    _parse_header_metadata(rows, exp)

    # Parse channel assignments and Ct summary table
    ct_table_row = _find_ct_table_start(rows)
    if ct_table_row is not None:
        _parse_channel_assignments(rows, ct_table_row, exp)
        _parse_ct_table(rows, ct_table_row, exp)

    # Parse per-run details (sequence, sample setup, etc.)
    _parse_run_details(rows, exp)

    # Parse reagents from header area
    _parse_reagents(rows, exp)

    return exp


def parse_experiment_grid(rows: list[list[str]], source_name: str = "") -> Experiment:
    """Parse an experiment from a pre-loaded grid of cell values.

    Used when reading from Google Sheets API directly.

    Args:
        rows: 2D list of cell values.
        source_name: Name/ID of the source file.

    Returns:
        Parsed Experiment object.
    """
    exp = Experiment(source_file=source_name)
    exp.experiment_date = _extract_date_from_filename(source_name)

    _parse_header_metadata(rows, exp)

    ct_table_row = _find_ct_table_start(rows)
    if ct_table_row is not None:
        _parse_channel_assignments(rows, ct_table_row, exp)
        _parse_ct_table(rows, ct_table_row, exp)

    _parse_run_details(rows, exp)
    _parse_reagents(rows, exp)

    return exp


def _read_csv(filepath: Path) -> list[list[str]]:
    """Read CSV file into a 2D list of strings."""
    rows = []
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        logger.error(f"Failed to read CSV {filepath}: {e}")
    return rows


def _cell(rows: list[list[str]], r: int, c: int) -> str:
    """Safely get a cell value, returning empty string if out of bounds."""
    if r < 0 or r >= len(rows):
        return ""
    row = rows[r]
    if c < 0 or c >= len(row):
        return ""
    return row[c].strip()


def _extract_date_from_filename(filename: str) -> Optional[date]:
    """Extract experiment date from filename patterns like '01_05_2026' or '01_08_2026'."""
    # Match MM_DD_YYYY pattern
    m = re.search(r"(\d{2})_(\d{2})_(\d{4})", filename)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass
    return None


def _parse_header_metadata(rows: list[list[str]], exp: Experiment) -> None:
    """Extract Purpose, Experiments, Tester, Device, Notes, Resume from the header area."""
    for i, row in enumerate(rows):
        if i > 40:  # header metadata is always in the first ~30 rows
            break

        col_a = _cell(rows, i, 0).lower()

        if col_a == "purpose":
            exp.purpose = _cell(rows, i, 2)
        elif col_a.startswith("experiment"):
            # May span multiple rows
            lines = [_cell(rows, i, 2)]
            j = i + 1
            while j < len(rows) and j < i + 6:
                next_a = _cell(rows, j, 0).lower()
                if next_a and next_a not in ("", ) and any(
                    next_a.startswith(k) for k in ["tester", "device", "notes", "resume", "fam", "rox"]
                ):
                    break
                val = _cell(rows, j, 2)
                if val:
                    lines.append(val)
                j += 1
            exp.experiments_desc = "\n".join(lines)
        elif col_a.startswith("tester"):
            exp.tester = _cell(rows, i, 2)
        elif col_a == "device":
            exp.device = _cell(rows, i, 2)
        elif col_a.startswith("notes"):
            # Notes may span multiple rows
            lines = [_cell(rows, i, 2)]
            j = i + 1
            while j < len(rows) and j < i + 10:
                next_a = _cell(rows, j, 0).lower()
                if next_a and any(
                    next_a.startswith(k) for k in ["resume", "video", "fam", "rox", "device", "tester"]
                ):
                    break
                val = _cell(rows, j, 2)
                if val:
                    lines.append(val)
                j += 1
            exp.notes = "\n".join(lines)
        elif col_a.startswith("resume"):
            lines = [_cell(rows, i, 2)]
            j = i + 1
            while j < len(rows) and j < i + 10:
                next_a = _cell(rows, j, 0).lower()
                if next_a and any(
                    next_a.startswith(k) for k in ["fam", "rox", "notes"]
                ):
                    break
                val = _cell(rows, j, 2)
                if val:
                    lines.append(val)
                j += 1
            exp.resume = "\n".join(lines)


def _find_ct_table_start(rows: list[list[str]]) -> Optional[int]:
    """Find the row where the FAM/ROX Ct summary table starts.

    Looks for a row with 'FAM' in col A and 'TRIAL' or 'RUN ID' nearby.
    """
    for i, row in enumerate(rows):
        col_a = _cell(rows, i, 0).upper()
        if col_a == "FAM":
            # Verify this is the Ct table header by checking for TRIAL/RUN ID
            row_text = " ".join(c.upper() for c in row if c.strip())
            if "TRIAL" in row_text or "RUN ID" in row_text:
                return i
    return None


def _parse_channel_assignments(
    rows: list[list[str]], ct_table_row: int, exp: Experiment
) -> None:
    """Parse channel assignments from the rows below the FAM header.

    The channel assignment rows look like:
      CH 0  | <label>  | ...
      CH 1  | <label>  | ...
    """
    # FAM channels
    for offset in range(1, 6):
        r = ct_table_row + offset
        col_a = _cell(rows, r, 0).upper().strip()
        if col_a.startswith("CH"):
            ch_num = _extract_channel_num(col_a)
            if ch_num is not None:
                label = _cell(rows, r, 1)
                exp.channel_assignments.append(
                    ChannelAssignment(channel_num=ch_num, label=label, fluorophore="FAM")
                )

    # ROX channels (after FAM section in same table)
    # Look for "ROX" row after the FAM channels
    for offset in range(5, 12):
        r = ct_table_row + offset
        col_a = _cell(rows, r, 0).upper().strip()
        if col_a == "ROX":
            # ROX header found, parse channels after it
            for rox_offset in range(1, 6):
                rr = r + rox_offset
                rox_a = _cell(rows, rr, 0).upper().strip()
                if rox_a.startswith("CH"):
                    ch_num = _extract_channel_num(rox_a)
                    if ch_num is not None:
                        label = _cell(rows, rr, 1)
                        exp.channel_assignments.append(
                            ChannelAssignment(
                                channel_num=ch_num, label=label, fluorophore="ROX"
                            )
                        )
            break


def _extract_channel_num(text: str) -> Optional[int]:
    """Extract channel number from text like 'CH 0', 'CH 1', 'CH 4'."""
    m = re.search(r"CH\s*(\d)", text)
    return int(m.group(1)) if m else None


def _find_ct_columns(rows: list[list[str]], ct_table_row: int) -> tuple[int, int, int, int]:
    """Find the column indices for FAM Ct start, FAM Ct end, ROX Ct start, ROX Ct end.

    Returns (fam_start, fam_end, rox_start, rox_end) as column indices.
    """
    # The header row is ct_table_row + 1 (CH 0 row with "Ch0 Ct", "Ch1 Ct", etc.)
    header_row = ct_table_row + 1
    if header_row >= len(rows):
        return (6, 11, 11, 16)

    row = rows[header_row]

    fam_start = None
    rox_start = None
    ct_positions = []

    for c, val in enumerate(row):
        v = val.strip().lower()
        if "ct" in v and ("ch0" in v or "ch 0" in v.replace("ch0", "ch0")):
            ct_positions.append(c)

    if len(ct_positions) >= 2:
        fam_start = ct_positions[0]
        rox_start = ct_positions[1]
    elif len(ct_positions) == 1:
        fam_start = ct_positions[0]
        rox_start = fam_start + 5

    if fam_start is None:
        # Fallback: scan for any cell containing "Ct"
        for c, val in enumerate(row):
            if "Ct" in val or "ct" in val.lower():
                if fam_start is None:
                    fam_start = c
                else:
                    rox_start = c
                    break

    # Default positions if detection fails
    if fam_start is None:
        fam_start = 6
    if rox_start is None:
        rox_start = fam_start + 5

    return (fam_start, fam_start + 5, rox_start, rox_start + 5)


def _parse_ct_value(val: str) -> Optional[float]:
    """Parse a Ct value string.

    Returns:
        float if valid Ct, 0.0 if "0" or "0.00", None if "-" or empty.
    """
    val = val.strip()
    if not val or val == "-":
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _parse_ct_table(rows: list[list[str]], ct_table_row: int, exp: Experiment) -> None:
    """Parse the Ct summary table to extract runs with FAM and ROX Ct values."""
    fam_start, fam_end, rox_start, rox_end = _find_ct_columns(rows, ct_table_row)

    # Find the TRIAL and RUN ID columns
    header_main = rows[ct_table_row] if ct_table_row < len(rows) else []
    trial_col = None
    runid_col = None
    notes_col = None

    for c, val in enumerate(header_main):
        v = val.strip().upper()
        if v == "TRIAL":
            trial_col = c
        elif "RUN ID" in v or "RUN_ID" in v:
            runid_col = c
        elif v == "NOTES":
            notes_col = c

    # If not found in header row, check the positions from the CH 0 row
    if trial_col is None:
        trial_col = 2
    if runid_col is None:
        runid_col = 3

    # Parse data rows (start at ct_table_row + 2, skip the CH header rows)
    # Data rows have trial numbers; scan until we hit empty trial/run IDs
    r = ct_table_row + 2
    while r < len(rows) and r < ct_table_row + 15:
        trial_str = _cell(rows, r, trial_col)
        run_id = _cell(rows, r, runid_col)

        if not trial_str and not run_id:
            r += 1
            continue

        # Skip channel label rows (CH 0, ROX, etc.)
        col_a = _cell(rows, r, 0).upper()
        if col_a in ("ROX", "") and not run_id:
            r += 1
            continue

        if not run_id:
            r += 1
            continue

        try:
            trial_num = int(trial_str) if trial_str else 0
        except ValueError:
            trial_num = 0

        ct_fam = CtValues(
            ch0=_parse_ct_value(_cell(rows, r, fam_start)),
            ch1=_parse_ct_value(_cell(rows, r, fam_start + 1)),
            ch2=_parse_ct_value(_cell(rows, r, fam_start + 2)),
            ch3=_parse_ct_value(_cell(rows, r, fam_start + 3)),
            ch4=_parse_ct_value(_cell(rows, r, fam_start + 4)),
        )

        ct_rox = CtValues(
            ch0=_parse_ct_value(_cell(rows, r, rox_start)),
            ch1=_parse_ct_value(_cell(rows, r, rox_start + 1)),
            ch2=_parse_ct_value(_cell(rows, r, rox_start + 2)),
            ch3=_parse_ct_value(_cell(rows, r, rox_start + 3)),
            ch4=_parse_ct_value(_cell(rows, r, rox_start + 4)),
        )

        notes = ""
        if notes_col is not None:
            notes = _cell(rows, r, notes_col)

        run = Run(
            trial_num=trial_num,
            run_id=run_id,
            ct_fam=ct_fam,
            ct_rox=ct_rox,
            notes=notes,
        )
        exp.runs.append(run)
        r += 1


def _parse_run_details(rows: list[list[str]], exp: Experiment) -> None:
    """Parse per-run detail sections (RUN ID: N blocks) for sample setup and sequence."""
    run_detail_starts = []

    for i, row in enumerate(rows):
        col_a = _cell(rows, i, 0).upper()
        if col_a.startswith("RUN ID:") or col_a == "RUN ID:":
            # Could be "RUN ID: 1" in col A, or "RUN ID:" in col A with number in col B
            run_num_str = col_a.replace("RUN ID:", "").strip()
            if not run_num_str:
                run_num_str = _cell(rows, i, 1).strip()
            run_id_value = _cell(rows, i, 2)
            run_detail_starts.append((i, run_num_str, run_id_value))

    for idx, (start_row, run_num_str, run_id_value) in enumerate(run_detail_starts):
        # Determine end of this run's section
        if idx + 1 < len(run_detail_starts):
            end_row = run_detail_starts[idx + 1][0]
        else:
            end_row = min(start_row + 100, len(rows))

        # Find matching run by run_id
        matching_run = None
        for run in exp.runs:
            if run.run_id == run_id_value:
                matching_run = run
                break

        if matching_run is None and run_id_value:
            # Create a stub run if not found in Ct table
            try:
                trial_num = int(run_num_str) if run_num_str else 0
            except ValueError:
                trial_num = 0
            matching_run = Run(trial_num=trial_num, run_id=run_id_value)
            # Don't add to exp.runs since we don't have Ct data

        # Parse details from this section
        for r in range(start_row + 1, end_row):
            col_a = _cell(rows, r, 0).lower()
            if col_a.startswith("sample setup"):
                if matching_run:
                    matching_run.sample_setup = _cell(rows, r, 2)
            elif col_a.startswith("batch number"):
                if matching_run:
                    matching_run.batch_number = _cell(rows, r, 2)
            elif "notes" in col_a and not col_a.startswith("run"):
                if matching_run and not matching_run.run_notes:
                    matching_run.run_notes = _cell(rows, r, 2)
            elif col_a.startswith("video"):
                if matching_run:
                    matching_run.video_file = _cell(rows, r, 2)
            elif col_a.startswith("report"):
                if matching_run:
                    matching_run.report_file = _cell(rows, r, 2)
            elif col_a.startswith("sequence setup"):
                if matching_run:
                    matching_run.sequence = _parse_sequence_section(rows, r)


def _parse_sequence_section(rows: list[list[str]], start_row: int) -> SequenceSetup:
    """Parse a Sequence Setup section starting at the given row."""
    chip_type = _cell(rows, start_row, 2)
    seq = SequenceSetup(chip_type=chip_type)

    # Next row should be the column headers: Step, Temp (C), Time (s), Cycle (times), Offset
    header_row = start_row + 1
    if header_row >= len(rows):
        return seq

    # Parse step rows after header
    r = header_row + 1
    current_step_name = ""

    while r < len(rows) and r < start_row + 15:
        col_b = _cell(rows, r, 2)  # Step name column
        col_c = _cell(rows, r, 3)  # Temp column
        col_d = _cell(rows, r, 4)  # Time column
        col_e = _cell(rows, r, 5)  # Cycle column
        col_f = _cell(rows, r, 6)  # Offset column

        # Empty row or new section
        if not col_b and not col_c:
            break

        if col_b:
            current_step_name = col_b

        if col_c:  # Has temperature data
            step = SequenceStep(
                step_name=current_step_name,
                temp_c=col_c,
                time_s=col_d,
                cycles=col_e,
                offset=col_f,
            )
            seq.steps.append(step)

        r += 1

    return seq


def _parse_reagents(rows: list[list[str]], exp: Experiment) -> None:
    """Parse reagent formulations from the header area.

    Reagents can appear in several formats:
    1. Single column (right side): Reagent name, Volume
    2. Per-channel columns: channel 0 ... channel 4, each with reagent lists
    """
    # Find "Reagents:" landmark in the first 5 rows
    reagent_col = None
    reagent_row = None

    for i in range(min(5, len(rows))):
        for c in range(len(rows[i])):
            val = rows[i][c].strip().lower()
            if val.startswith("reagent"):
                reagent_col = c
                reagent_row = i
                break
        if reagent_row is not None:
            break

    if reagent_row is None:
        # Try alternative: look for "Number of samples" or "Master mix"
        for i in range(min(5, len(rows))):
            for c in range(len(rows[i])):
                val = rows[i][c].strip().lower()
                if "number of samples" in val or "master mix" in val:
                    reagent_col = c
                    reagent_row = i
                    break
            if reagent_row is not None:
                break

    if reagent_row is None:
        return

    # Determine if per-channel or single column
    # Per-channel: "channel 0", "channel 1", etc. in the header row
    header_text = " ".join(c.strip().lower() for c in rows[reagent_row] if c.strip())
    is_per_channel = "channel 0" in header_text or "channel 1" in header_text

    if is_per_channel:
        _parse_per_channel_reagents(rows, reagent_row, reagent_col, exp)
    else:
        _parse_single_reagent_list(rows, reagent_row, reagent_col, exp)


def _parse_single_reagent_list(
    rows: list[list[str]], start_row: int, start_col: int, exp: Experiment
) -> None:
    """Parse a single-column reagent list."""
    formulation = ReagentFormulation()

    # Look for volume column header
    vol_col = None
    for i in range(start_row, min(start_row + 5, len(rows))):
        for c in range(start_col, min(start_col + 5, len(rows[i]))):
            val = rows[i][c].strip().lower()
            if "volume" in val or "ul" in val.lower():
                vol_col = c
                break
        if vol_col is not None:
            break

    # Default to start_col + 2 or + 3 based on common layout
    if vol_col is None:
        vol_col = start_col + 3

    # Name column is usually start_col or start_col + 1
    name_col = start_col + 1 if start_col + 1 < vol_col else start_col

    # Parse reagent rows
    r = start_row + 2  # skip header rows
    while r < len(rows) and r < start_row + 25:
        name = _cell(rows, r, name_col)
        if not name:
            name = _cell(rows, r, start_col)
        vol_str = _cell(rows, r, vol_col)

        if not name and not vol_str:
            r += 1
            continue

        if name.lower().startswith("total"):
            try:
                formulation.total_volume_uL = float(vol_str) if vol_str else 0
            except ValueError:
                pass
            break

        try:
            vol = float(vol_str) if vol_str else 0
        except ValueError:
            vol = 0

        if name and name.lower() not in ("reagent", "reagent description"):
            formulation.reagents.append(ReagentItem(name=name, volume_uL=vol))

        r += 1

    if formulation.reagents:
        exp.reagent_formulations.append(formulation)


def _parse_per_channel_reagents(
    rows: list[list[str]], start_row: int, start_col: int, exp: Experiment
) -> None:
    """Parse per-channel reagent formulations (5 channels side by side)."""
    # Find where each channel's columns start
    channel_starts = []
    for c in range(start_col, min(start_col + 30, len(rows[start_row]) if start_row < len(rows) else 0)):
        val = _cell(rows, start_row, c).lower()
        if val.startswith("channel") or "number of samples" in val:
            channel_starts.append(c)

    if not channel_starts:
        return

    # For each channel column group, parse the reagents
    for ch_idx, ch_col in enumerate(channel_starts):
        formulation = ReagentFormulation(channel=ch_idx)

        # Find "Number of samples" value
        num_samples_str = _cell(rows, start_row + 1, ch_col + 1) if start_row + 1 < len(rows) else ""
        try:
            formulation.num_samples = int(num_samples_str) if num_samples_str else None
        except ValueError:
            pass

        # Find the "Master mix" / "Volume" header row
        vol_col = ch_col + 1  # volume is typically next column

        # Parse reagent rows
        r = start_row + 3  # skip channel header + num samples + master mix header
        while r < len(rows) and r < start_row + 25:
            name = _cell(rows, r, ch_col)
            vol_str = _cell(rows, r, ch_col + 1)

            if not name and not vol_str:
                r += 1
                continue

            if name.lower().startswith("total"):
                try:
                    formulation.total_volume_uL = float(vol_str) if vol_str else 0
                except ValueError:
                    pass
                break

            try:
                vol = float(vol_str) if vol_str else 0
            except ValueError:
                vol = 0

            if name:
                formulation.reagents.append(ReagentItem(name=name, volume_uL=vol))

            r += 1

        if formulation.reagents:
            exp.reagent_formulations.append(formulation)


def experiment_to_summary_text(exp: Experiment) -> str:
    """Convert an Experiment to a human-readable text summary for AI analysis."""
    lines = []
    lines.append(f"### Experiment: {exp.source_file}")
    if exp.experiment_date:
        lines.append(f"**Date**: {exp.experiment_date.isoformat()}")
    lines.append(f"**Purpose**: {exp.purpose}")
    if exp.experiments_desc:
        lines.append(f"**Experiments**: {exp.experiments_desc}")
    lines.append(f"**Tester**: {exp.tester}")
    lines.append(f"**Device**: {exp.device}")
    if exp.notes:
        lines.append(f"**Notes**: {exp.notes}")

    # Channel assignments
    if exp.channel_assignments:
        lines.append("\n**Channel Assignments**:")
        for ca in exp.channel_assignments:
            if ca.label:
                lines.append(f"  - {ca.fluorophore} CH {ca.channel_num}: {ca.label}")

    # Ct values table
    if exp.runs:
        lines.append("\n**Ct Values**:")
        lines.append(
            "| Trial | Run ID | FAM Ch0 | FAM Ch1 | FAM Ch2 | FAM Ch3 | FAM Ch4 | "
            "ROX Ch0 | ROX Ch1 | ROX Ch2 | ROX Ch3 | ROX Ch4 | Notes |"
        )
        lines.append("|" + "---|" * 13)
        for run in exp.runs:
            fam = run.ct_fam
            rox = run.ct_rox

            def fmt(v: Optional[float]) -> str:
                if v is None:
                    return "-"
                if v == 0.0:
                    return "0.00"
                return f"{v:.2f}"

            lines.append(
                f"| {run.trial_num} | {run.run_id} | "
                f"{fmt(fam.ch0)} | {fmt(fam.ch1)} | {fmt(fam.ch2)} | {fmt(fam.ch3)} | {fmt(fam.ch4)} | "
                f"{fmt(rox.ch0)} | {fmt(rox.ch1)} | {fmt(rox.ch2)} | {fmt(rox.ch3)} | {fmt(rox.ch4)} | "
                f"{run.notes} |"
            )

    # Sequence info (from first run that has it)
    for run in exp.runs:
        if run.sequence and run.sequence.steps:
            lines.append(f"\n**Sequence Setup** ({run.sequence.chip_type}):")
            for step in run.sequence.steps:
                lines.append(
                    f"  - {step.step_name}: {step.temp_c}C, {step.time_s}s, "
                    f"{step.cycles} cycles, offset {step.offset}"
                )
            break

    # Resume
    if exp.resume:
        lines.append(f"\n**Resume/Conclusions**: {exp.resume}")

    return "\n".join(lines)
