"""Tests for the experiment sheet, goals, and journal parsers."""

import os
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parsers.experiment_sheet import parse_experiment_csv, experiment_to_summary_text
from src.parsers.goals import parse_goals_csv
from src.parsers.journal import parse_journal_txt, filter_entries_by_date_range

SAMPLE_DIR = Path(r"C:\Users\kyleh\OneDrive\Documents\Stampede Data")
SHEETS_DIR = SAMPLE_DIR / "Device Testing Sheets"
GOALS_DIR = SAMPLE_DIR / "Goals"
JOURNALS_DIR = SAMPLE_DIR / "Journals"


def test_parse_experiment_liquid_ts():
    """Test parsing the Liquid + TS experiment sheet."""
    exp = parse_experiment_csv(SHEETS_DIR / "Device Testing - H1 2026 - 01_05_2026 Liquid + TS.csv")

    assert exp.experiment_date == date(2026, 1, 5)
    assert exp.purpose == "Check LOD HS DSbio with real sample"
    assert exp.tester == "Adit"
    assert exp.device == "TS-003"
    assert len(exp.runs) == 4
    assert exp.runs[0].run_id == "0105_003_TS_6600_1"
    # Check Ct values
    assert exp.runs[0].ct_fam.ch2 == 24.63
    assert exp.runs[0].ct_rox.ch2 == 25.92
    assert exp.resume  # Should have resume text
    print("  PASS: Liquid + TS")


def test_parse_experiment_preheat():
    """Test parsing the Ftaq Preheat Seq Research sheet (wider format)."""
    exp = parse_experiment_csv(SHEETS_DIR / "Device Testing - H1 2026 - 01_08_2026 - Ftaq Preheat Seq Research.csv")

    assert exp.experiment_date == date(2026, 1, 8)
    assert exp.tester == "Bowo"
    assert exp.device == "TS-006"
    assert len(exp.runs) >= 6  # Has 8 runs in data
    # Check channel assignments have labels
    fam_channels = [ca for ca in exp.channel_assignments if ca.fluorophore == "FAM"]
    assert any("Normal reagents" in ca.label for ca in fam_channels)
    assert len(exp.reagent_formulations) >= 3  # Per-channel reagents
    print("  PASS: Preheat Seq Research")


def test_parse_experiment_lod():
    """Test parsing the LOD with TS sheet."""
    exp = parse_experiment_csv(SHEETS_DIR / "Device Testing - H1 2026 - DRAFT01_28_2026-Ftaq LOD with TS.csv")

    assert exp.experiment_date == date(2026, 1, 28)
    assert "Adit" in exp.tester
    assert len(exp.runs) == 6
    # Check LOD channel labels
    fam_channels = [ca for ca in exp.channel_assignments if ca.fluorophore == "FAM"]
    labels = [ca.label for ca in fam_channels]
    assert any("6600 cp" in l for l in labels)
    assert any("NC" in l for l in labels)
    # Check sequence has touchdown
    for run in exp.runs:
        if run.sequence:
            step_names = [s.step_name for s in run.sequence.steps]
            assert any("Touchdown" in n for n in step_names), f"Expected Touchdown step, got {step_names}"
            break
    print("  PASS: LOD with TS")


def test_parse_all_sheets_no_errors():
    """Test that all 22 unique sheets parse without errors."""
    import glob

    files = sorted(glob.glob(str(SHEETS_DIR / "*.csv")))
    unique_files = [f for f in files if "Copy" not in os.path.basename(f)]

    errors = []
    for f in unique_files:
        try:
            exp = parse_experiment_csv(f)
            assert exp.runs, f"No runs parsed from {os.path.basename(f)}"
        except Exception as e:
            errors.append((os.path.basename(f), str(e)))

    assert not errors, f"Parser errors: {errors}"
    print(f"  PASS: All {len(unique_files)} sheets parsed successfully")


def test_experiment_to_summary():
    """Test summary text generation."""
    exp = parse_experiment_csv(SHEETS_DIR / "Device Testing - H1 2026 - 01_05_2026 Liquid + TS.csv")
    text = experiment_to_summary_text(exp)
    assert "Purpose" in text
    assert "Ct Values" in text
    assert "0105_003_TS_6600_1" in text
    print("  PASS: Summary text generation")


def test_parse_goals():
    """Test goals CSV parsing."""
    goals = parse_goals_csv(GOALS_DIR / "Stampede Goals H1 2026 - Goals.csv")
    assert len(goals) >= 5  # Should have at least 5 scored goals

    goal_names = [g.short_name for g in goals]
    assert "Clinical Verification Study" in goal_names
    assert "R2D2" in goal_names

    clinical = next(g for g in goals if g.short_name == "Clinical Verification Study")
    assert clinical.points == 50
    assert "RSPAW" in clinical.requirements

    r2d2 = next(g for g in goals if g.short_name == "R2D2")
    assert r2d2.points == 50
    print("  PASS: Goals parsing")


def test_parse_journal_rnd():
    """Test R&D journal parsing."""
    entries = parse_journal_txt(JOURNALS_DIR / "STAMPEDE - H1 2026 RnD Journal.txt")
    assert len(entries) > 10

    # Check that dates parse correctly
    for e in entries:
        assert e.entry_date is not None
        assert e.entry_date.year >= 2025  # H1 2026 journal may reference H2 2025

    # Check author detection
    authors = set(e.author for e in entries if e.author)
    assert "Bowo" in authors or "Dwi" in authors or "Kabir" in authors
    print(f"  PASS: R&D Journal ({len(entries)} entries)")


def test_parse_journal_sw():
    """Test SW journal parsing."""
    entries = parse_journal_txt(JOURNALS_DIR / "Stampede SW Jourmal.txt")
    assert len(entries) > 5

    # SW journal uses YYYY-MM-DD format
    for e in entries:
        assert e.entry_date is not None
    print(f"  PASS: SW Journal ({len(entries)} entries)")


def test_filter_entries_by_date():
    """Test date range filtering."""
    entries = parse_journal_txt(JOURNALS_DIR / "STAMPEDE - H1 2026 RnD Journal.txt")
    filtered = filter_entries_by_date_range(
        entries, date(2026, 2, 1), date(2026, 2, 4)
    )
    assert len(filtered) > 0
    for e in filtered:
        assert date(2026, 2, 1) <= e.entry_date <= date(2026, 2, 4)
    print(f"  PASS: Date filtering ({len(filtered)} entries in range)")


if __name__ == "__main__":
    print("Running parser tests...\n")
    test_parse_experiment_liquid_ts()
    test_parse_experiment_preheat()
    test_parse_experiment_lod()
    test_parse_all_sheets_no_errors()
    test_experiment_to_summary()
    test_parse_goals()
    test_parse_journal_rnd()
    test_parse_journal_sw()
    test_filter_entries_by_date()
    print("\nAll tests passed!")
