"""Chart generation for the Stampede weekly report.

Uses matplotlib + seaborn with colorblind-friendly palettes.
Only compares like-to-like within the same experiment family.
"""

import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend for server use
import matplotlib.pyplot as plt
import numpy as np

from src.models.data import Experiment, Goal, Run

logger = logging.getLogger(__name__)

# Colorblind-friendly palette (IBM Design / Wong palette)
CB_PALETTE = [
    "#0072B2",  # blue
    "#E69F00",  # orange
    "#009E73",  # green
    "#CC79A7",  # pink
    "#56B4E9",  # sky blue
    "#D55E00",  # vermillion
    "#F0E442",  # yellow
    "#000000",  # black
]
COLORS = {
    "green": CB_PALETTE[2],
    "yellow": CB_PALETTE[6],
    "red": CB_PALETTE[5],
    "blue": CB_PALETTE[0],
    "orange": CB_PALETTE[1],
    "purple": CB_PALETTE[3],
    "gray": "#999999",
}


def generate_all_charts(
    experiments: list[Experiment],
    goals: list[Goal],
    goal_assessment: str,
    output_dir: Path,
) -> list[Path]:
    """Generate all applicable charts for the weekly report.

    Args:
        experiments: This week's parsed experiments.
        goals: Team goals.
        goal_assessment: AI-generated goal progress text.
        output_dir: Directory to save chart PNG files.

    Returns:
        List of generated chart file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_paths = []

    # 1. LOD Curves (if LOD experiments exist)
    lod_exps = [e for e in experiments if _is_lod_experiment(e)]
    if lod_exps:
        path = _generate_lod_chart(lod_exps, output_dir)
        if path:
            chart_paths.append(path)

    # 2. Ct Comparison (for experiments with multiple runs)
    multi_run = [e for e in experiments if len(e.runs) >= 2]
    if multi_run:
        path = _generate_ct_comparison(multi_run, output_dir)
        if path:
            chart_paths.append(path)

    # 3. Goal Progress Dashboard
    if goals:
        path = _generate_goal_dashboard(goals, goal_assessment, output_dir)
        if path:
            chart_paths.append(path)

    # 4. Weekly Activity Summary
    if experiments:
        path = _generate_activity_summary(experiments, output_dir)
        if path:
            chart_paths.append(path)

    # 5. Replicate Consistency
    if multi_run:
        path = _generate_replicate_consistency(multi_run, output_dir)
        if path:
            chart_paths.append(path)

    return chart_paths


def _is_lod_experiment(exp: Experiment) -> bool:
    """Check if an experiment is an LOD (limit of detection) test."""
    text = f"{exp.purpose} {exp.experiments_desc} {exp.source_file}".lower()
    return "lod" in text or "limit of detection" in text or "cp" in " ".join(
        ca.label.lower() for ca in exp.channel_assignments
    )


def _extract_copy_numbers(exp: Experiment) -> list[float]:
    """Extract copy numbers from channel assignments (e.g., 'IS 6600 cp')."""
    copies = []
    for ca in exp.channel_assignments:
        m = re.search(r"([\d.]+)\s*cp", ca.label, re.IGNORECASE)
        if m:
            copies.append(float(m.group(1)))
    return copies


def _get_fam_ct_list(run: Run) -> list[Optional[float]]:
    """Get FAM Ct values as a list [ch0, ch1, ch2, ch3, ch4]."""
    return [run.ct_fam.ch0, run.ct_fam.ch1, run.ct_fam.ch2, run.ct_fam.ch3, run.ct_fam.ch4]


def _generate_lod_chart(experiments: list[Experiment], output_dir: Path) -> Optional[Path]:
    """Generate LOD curve chart (Ct vs copy number)."""
    fig, ax = plt.subplots(figsize=(10, 6))
    plt.style.use("seaborn-v0_8-whitegrid")

    has_data = False
    for exp_idx, exp in enumerate(experiments):
        copies = _extract_copy_numbers(exp)
        if not copies:
            continue

        for run in exp.runs:
            cts = _get_fam_ct_list(run)
            # Map channels to copy numbers
            plot_copies = []
            plot_cts = []
            for i, (cp, ct) in enumerate(zip(copies, cts)):
                if ct is not None and ct > 0:
                    plot_copies.append(cp)
                    plot_cts.append(ct)

            if plot_copies:
                has_data = True
                label = f"{run.run_id}" if len(experiments) == 1 else f"{exp.source_file[:30]}|{run.run_id}"
                ax.plot(
                    plot_copies, plot_cts,
                    marker="o", linewidth=1.5,
                    color=CB_PALETTE[exp_idx % len(CB_PALETTE)],
                    alpha=0.7,
                    label=label[:40],
                )

    if not has_data:
        plt.close(fig)
        return None

    ax.set_xscale("log")
    ax.set_xlabel("Copies per Reaction", fontsize=12)
    ax.set_ylabel("Ct Value", fontsize=12)
    ax.set_title("Limit of Detection (LOD) Curves", fontsize=14, fontweight="bold")
    ax.invert_yaxis()  # Lower Ct = better, so invert
    ax.legend(fontsize=8, loc="best")

    path = output_dir / "lod_curves.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _generate_ct_comparison(experiments: list[Experiment], output_dir: Path) -> Optional[Path]:
    """Generate Ct value comparison chart across runs within experiments."""
    fig, axes = plt.subplots(
        1, min(len(experiments), 3),
        figsize=(5 * min(len(experiments), 3), 6),
        squeeze=False,
    )

    for idx, exp in enumerate(experiments[:3]):
        ax = axes[0, idx]
        run_ids = []
        ct_values = []

        for run in exp.runs:
            cts = _get_fam_ct_list(run)
            valid_cts = [c for c in cts if c is not None and c > 0]
            if valid_cts:
                run_ids.append(run.run_id[-15:])
                ct_values.append(valid_cts)

        if not run_ids:
            ax.text(0.5, 0.5, "No Ct data", ha="center", va="center", transform=ax.transAxes)
            continue

        # Box plot for each run
        bp = ax.boxplot(ct_values, labels=run_ids, patch_artist=True)
        for patch in bp["boxes"]:
            patch.set_facecolor(CB_PALETTE[idx % len(CB_PALETTE)])
            patch.set_alpha(0.6)

        ax.set_ylabel("Ct Value" if idx == 0 else "")
        ax.set_title(exp.source_file[-40:] if len(exp.source_file) > 40 else exp.source_file, fontsize=9)
        ax.tick_params(axis="x", rotation=45)

    fig.suptitle("Ct Value Distribution by Run", fontsize=14, fontweight="bold")
    fig.tight_layout()

    path = output_dir / "ct_comparison.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _generate_goal_dashboard(
    goals: list[Goal], goal_assessment: str, output_dir: Path
) -> Optional[Path]:
    """Generate goal progress dashboard chart."""
    fig, ax = plt.subplots(figsize=(10, max(3, len(goals) * 0.8)))
    plt.style.use("seaborn-v0_8-whitegrid")

    # Parse goal assessment to estimate progress percentages
    goal_names = []
    progress = []
    colors = []
    due_labels = []

    for g in goals:
        if g.points <= 0:
            continue

        goal_names.append(g.short_name[:30])
        due_labels.append(g.due_date[:20] if g.due_date else "TBD")

        # Try to extract progress from AI assessment
        pct = _estimate_goal_progress(g.short_name, goal_assessment)
        progress.append(pct)

        # Color based on progress and due date
        if pct >= 70:
            colors.append(COLORS["green"])
        elif pct >= 40:
            colors.append(COLORS["yellow"])
        else:
            colors.append(COLORS["red"])

    if not goal_names:
        plt.close(fig)
        return None

    y_pos = range(len(goal_names))
    bars = ax.barh(y_pos, progress, color=colors, edgecolor="white", height=0.6)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(goal_names, fontsize=10)
    ax.set_xlabel("Estimated Progress (%)", fontsize=12)
    ax.set_xlim(0, 105)
    ax.set_title("Goal Progress Dashboard", fontsize=14, fontweight="bold")

    # Add annotations
    for i, (bar, due) in enumerate(zip(bars, due_labels)):
        width = bar.get_width()
        ax.text(
            width + 1, bar.get_y() + bar.get_height() / 2,
            f"{width:.0f}% | Due: {due}",
            ha="left", va="center", fontsize=8,
        )

    fig.tight_layout()
    path = output_dir / "goal_dashboard.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _estimate_goal_progress(goal_name: str, assessment: str) -> float:
    """Estimate goal completion percentage from AI assessment text."""
    # Look for percentage mentions near the goal name
    pattern = rf"{re.escape(goal_name[:15])}.*?(\d{{1,3}})\s*%"
    m = re.search(pattern, assessment, re.IGNORECASE | re.DOTALL)
    if m:
        return min(float(m.group(1)), 100)

    # Default estimates based on common patterns
    lower = assessment.lower()
    name_lower = goal_name.lower()
    if name_lower in lower:
        if "complete" in lower or "achieved" in lower:
            return 80
        elif "in progress" in lower or "ongoing" in lower:
            return 40
        elif "not started" in lower or "at risk" in lower:
            return 15

    return 25  # default


def _generate_activity_summary(experiments: list[Experiment], output_dir: Path) -> Optional[Path]:
    """Generate weekly experiment activity summary infographic."""
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))

    # 1. Experiments count by type/family
    ax1 = axes[0]
    families = defaultdict(int)
    for exp in experiments:
        # Infer family from filename
        name = exp.source_file.lower()
        if "preheat" in name:
            families["Preheat Seq"] += 1
        elif "evagreen" in name:
            families["Evagreen"] += 1
        elif "anneal" in name:
            families["Anneal Temp"] += 1
        elif "cross" in name or "rxn" in name:
            families["Cross Rxn"] += 1
        elif "lod" in name:
            families["LOD Testing"] += 1
        elif "sputum" in name:
            families["Sputum"] += 1
        elif "msm" in name:
            families["MSM"] += 1
        else:
            families["Other"] += 1

    if families:
        names = list(families.keys())
        counts = list(families.values())
        ax1.barh(names, counts, color=CB_PALETTE[:len(names)])
        ax1.set_xlabel("Count")
        ax1.set_title("Experiments by Family", fontweight="bold")

    # 2. Tester activity
    ax2 = axes[1]
    testers = defaultdict(int)
    for exp in experiments:
        for t in exp.tester.split(","):
            t = t.strip()
            if t:
                testers[t] += 1

    if testers:
        names = list(testers.keys())
        counts = list(testers.values())
        ax2.barh(names, counts, color=CB_PALETTE[:len(names)])
        ax2.set_xlabel("Experiments")
        ax2.set_title("Scientist Activity", fontweight="bold")

    # 3. Device usage
    ax3 = axes[2]
    devices = defaultdict(int)
    for exp in experiments:
        if exp.device:
            devices[exp.device] += 1

    if devices:
        names = list(devices.keys())
        counts = list(devices.values())
        ax3.barh(names, counts, color=CB_PALETTE[:len(names)])
        ax3.set_xlabel("Experiments")
        ax3.set_title("Device Usage", fontweight="bold")

    fig.suptitle(
        f"Weekly Activity: {len(experiments)} experiments, "
        f"{sum(len(e.runs) for e in experiments)} total runs",
        fontsize=13, fontweight="bold",
    )
    fig.tight_layout()

    path = output_dir / "activity_summary.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _generate_replicate_consistency(
    experiments: list[Experiment], output_dir: Path
) -> Optional[Path]:
    """Generate replicate consistency dot plot."""
    fig, ax = plt.subplots(figsize=(10, 6))
    plt.style.use("seaborn-v0_8-whitegrid")

    y_labels = []
    x_data = []
    has_data = False

    for exp in experiments:
        for run in exp.runs:
            cts = _get_fam_ct_list(run)
            valid = [c for c in cts if c is not None and c > 0]
            if len(valid) >= 2:
                has_data = True
                label = run.run_id[-20:]
                y_labels.append(label)
                x_data.append(valid)

    if not has_data:
        plt.close(fig)
        return None

    for i, (label, cts) in enumerate(zip(y_labels[-15:], x_data[-15:])):
        ax.scatter(cts, [i] * len(cts), color=CB_PALETTE[0], alpha=0.7, s=60)
        mean_ct = np.mean(cts)
        std_ct = np.std(cts)
        ax.plot(mean_ct, i, "D", color=CB_PALETTE[3], markersize=8)
        ax.annotate(
            f"SD={std_ct:.1f}", (max(cts) + 0.5, i),
            fontsize=7, va="center",
        )

    ax.set_yticks(range(len(y_labels[-15:])))
    ax.set_yticklabels(y_labels[-15:], fontsize=8)
    ax.set_xlabel("Ct Value", fontsize=12)
    ax.set_title("Replicate Consistency (FAM channels)", fontsize=14, fontweight="bold")

    fig.tight_layout()
    path = output_dir / "replicate_consistency.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path
