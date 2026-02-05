"""Google Slides report builder.

Generates a weekly report presentation and uploads it to Google Drive.
"""

import logging
import re
from datetime import date
from pathlib import Path
from typing import Optional
from uuid import uuid4

from googleapiclient.http import MediaFileUpload

from src.drive.client import DriveClient
from src.models.data import AnalysisResult, Experiment, RecommendationResult

logger = logging.getLogger(__name__)

# Slide dimensions (standard 16:9 in EMU - English Metric Units)
SLIDE_WIDTH = 9144000   # 10 inches
SLIDE_HEIGHT = 5143500  # 5.625 inches


def create_weekly_report(
    drive_client: DriveClient,
    week_start: date,
    week_end: date,
    analysis: AnalysisResult,
    recommendations: RecommendationResult,
    experiments: list[Experiment],
    chart_paths: list[Path],
    reports_folder_id: str,
) -> str:
    """Create a Google Slides weekly report and upload to Drive.

    Args:
        drive_client: Authenticated Drive client.
        week_start: Start date of the reporting week.
        week_end: End date of the reporting week.
        analysis: Stage 1 analysis results.
        recommendations: Stage 2 recommendation results.
        experiments: This week's parsed experiments.
        chart_paths: Paths to generated chart PNGs.
        reports_folder_id: Drive folder ID for output.

    Returns:
        URL of the created presentation.
    """
    slides_service = drive_client.slides
    drive_service = drive_client.drive

    title = f"Stampede Weekly Report - {week_start.strftime('%b %d')} to {week_end.strftime('%b %d, %Y')}"

    # Create a new presentation
    presentation = slides_service.presentations().create(
        body={"title": title}
    ).execute()
    presentation_id = presentation["presentationId"]

    # Move to reports folder
    try:
        file = drive_service.files().get(
            fileId=presentation_id, fields="parents"
        ).execute()
        previous_parents = ",".join(file.get("parents", []))
        drive_service.files().update(
            fileId=presentation_id,
            addParents=reports_folder_id,
            removeParents=previous_parents,
            fields="id, parents",
        ).execute()
    except Exception as e:
        logger.warning(f"Could not move presentation to reports folder: {e}")

    # Build all slides
    requests = []

    # Remove the default blank slide
    default_slides = presentation.get("slides", [])
    if default_slides:
        requests.append({
            "deleteObject": {"objectId": default_slides[0]["objectId"]}
        })

    # 1. Title Slide
    requests.extend(_create_title_slide(title, week_start, week_end))

    # 2. Executive Summary
    exec_summary = _extract_section(analysis.raw_response, "EXECUTIVE SUMMARY")
    requests.extend(_create_text_slide("Executive Summary", exec_summary))

    # 3. Experiments This Week
    exp_table = _build_experiment_table_text(experiments)
    requests.extend(_create_text_slide("Experiments This Week", exp_table))

    # 4. Key Results (with charts)
    exp_analysis = _extract_section(analysis.raw_response, "EXPERIMENT-BY-EXPERIMENT ANALYSIS")
    requests.extend(_create_text_slide("Key Results", exp_analysis[:3000]))

    # 5. Anomalies & Contradictions
    anomalies = _extract_section(analysis.raw_response, "CONTRADICTION")
    if anomalies:
        requests.extend(_create_text_slide("Anomalies & Contradictions", anomalies[:2000]))

    # 6. Cross-Experiment Insights
    insights = _extract_section(analysis.raw_response, "CROSS-EXPERIMENT INSIGHTS")
    if insights:
        requests.extend(_create_text_slide("Cross-Experiment Insights", insights[:2000]))

    # 7. Goal Progress
    goal_text = _extract_section(recommendations.raw_response, "GOAL URGENCY")
    requests.extend(_create_text_slide("Goal Progress", goal_text[:2000]))

    # 8. Strategic Direction
    strategy = _extract_section(recommendations.raw_response, "STRATEGIC DIRECTION")
    requests.extend(_create_text_slide("Strategic Direction", strategy[:2000]))

    # 9. Recommended Experiments
    recs = _extract_section(recommendations.raw_response, "SPECIFIC EXPERIMENT RECOMMENDATIONS")
    requests.extend(_create_text_slide("Recommended Next Experiments", recs[:3000]))

    # 10. Experiments to Avoid
    avoid = _extract_section(recommendations.raw_response, "EXPERIMENTS TO AVOID")
    if avoid:
        requests.extend(_create_text_slide("Experiments to Avoid", avoid[:2000]))

    # Execute all slide creation requests
    if requests:
        slides_service.presentations().batchUpdate(
            presentationId=presentation_id,
            body={"requests": requests},
        ).execute()

    # Upload chart images to slides
    _add_chart_images(drive_client, presentation_id, chart_paths)

    url = f"https://docs.google.com/presentation/d/{presentation_id}"
    logger.info(f"Created report: {url}")
    return url


def _create_title_slide(title: str, week_start: date, week_end: date) -> list[dict]:
    """Create title slide requests."""
    slide_id = _new_id()
    title_id = _new_id()
    subtitle_id = _new_id()

    return [
        {
            "createSlide": {
                "objectId": slide_id,
                "insertionIndex": 0,
                "slideLayoutReference": {"predefinedLayout": "TITLE"},
                "placeholderIdMappings": [
                    {"layoutPlaceholder": {"type": "CENTERED_TITLE"}, "objectId": title_id},
                    {"layoutPlaceholder": {"type": "SUBTITLE"}, "objectId": subtitle_id},
                ],
            }
        },
        {
            "insertText": {
                "objectId": title_id,
                "text": title,
            }
        },
        {
            "insertText": {
                "objectId": subtitle_id,
                "text": f"Weekly Report: {week_start.strftime('%B %d')} - {week_end.strftime('%B %d, %Y')}\nAuto-generated by Stampede Report Pipeline",
            }
        },
    ]


def _create_text_slide(title: str, body_text: str) -> list[dict]:
    """Create a text content slide with title and body."""
    slide_id = _new_id()
    title_id = _new_id()
    body_id = _new_id()

    # Truncate body text to avoid API limits
    if len(body_text) > 3000:
        body_text = body_text[:2950] + "\n\n[... continued in appendix ...]"

    return [
        {
            "createSlide": {
                "objectId": slide_id,
                "slideLayoutReference": {"predefinedLayout": "TITLE_AND_BODY"},
                "placeholderIdMappings": [
                    {"layoutPlaceholder": {"type": "TITLE"}, "objectId": title_id},
                    {"layoutPlaceholder": {"type": "BODY"}, "objectId": body_id},
                ],
            }
        },
        {
            "insertText": {
                "objectId": title_id,
                "text": title,
            }
        },
        {
            "insertText": {
                "objectId": body_id,
                "text": body_text or "No data available for this section.",
            }
        },
        # Style body text smaller
        {
            "updateTextStyle": {
                "objectId": body_id,
                "style": {"fontSize": {"magnitude": 10, "unit": "PT"}},
                "textRange": {"type": "ALL"},
                "fields": "fontSize",
            }
        },
    ]


def _add_chart_images(
    drive_client: DriveClient,
    presentation_id: str,
    chart_paths: list[Path],
) -> None:
    """Upload chart images and add them as slides."""
    slides_service = drive_client.slides

    for chart_path in chart_paths:
        if not chart_path.exists():
            continue

        # Upload image to Drive (temporarily)
        try:
            image_id = drive_client.upload_file(
                str(chart_path),
                folder_id="root",  # temp location
                mime_type="image/png",
                name=f"stampede_chart_{chart_path.stem}.png",
            )

            # Make the file publicly readable (needed for Slides to embed)
            drive_client.drive.permissions().create(
                fileId=image_id,
                body={"type": "anyone", "role": "reader"},
            ).execute()

            image_url = f"https://drive.google.com/uc?id={image_id}"

            # Create a new slide with the image
            slide_id = _new_id()
            title_id = _new_id()
            image_obj_id = _new_id()

            chart_title = chart_path.stem.replace("_", " ").title()

            requests = [
                {
                    "createSlide": {
                        "objectId": slide_id,
                        "slideLayoutReference": {"predefinedLayout": "BLANK"},
                    }
                },
                {
                    "createImage": {
                        "objectId": image_obj_id,
                        "url": image_url,
                        "elementProperties": {
                            "pageObjectId": slide_id,
                            "size": {
                                "width": {"magnitude": 8000000, "unit": "EMU"},
                                "height": {"magnitude": 4500000, "unit": "EMU"},
                            },
                            "transform": {
                                "scaleX": 1,
                                "scaleY": 1,
                                "translateX": 572000,
                                "translateY": 500000,
                                "unit": "EMU",
                            },
                        },
                    }
                },
            ]

            slides_service.presentations().batchUpdate(
                presentationId=presentation_id,
                body={"requests": requests},
            ).execute()

        except Exception as e:
            logger.warning(f"Failed to add chart {chart_path.name}: {e}")


def _extract_section(text: str, section_name: str) -> str:
    """Extract a section from the AI response text by heading."""
    # Try numbered section pattern: "1. SECTION NAME" or "## SECTION NAME"
    patterns = [
        rf"\d+\.\s*{re.escape(section_name)}.*?\n(.*?)(?=\n\d+\.\s+[A-Z]|\Z)",
        rf"#+\s*{re.escape(section_name)}.*?\n(.*?)(?=\n#+\s+|\Z)",
        rf"{re.escape(section_name)}.*?\n(.*?)(?=\n[A-Z]{{2,}}|\Z)",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if m:
            return m.group(1).strip()

    # Fallback: search for section name and take text until next all-caps heading
    idx = text.lower().find(section_name.lower())
    if idx >= 0:
        # Find start of content (after the heading line)
        newline = text.find("\n", idx)
        if newline >= 0:
            remaining = text[newline + 1:]
            # Take until next major heading
            next_heading = re.search(r"\n\d+\.\s+[A-Z]", remaining)
            if next_heading:
                return remaining[:next_heading.start()].strip()
            return remaining[:2000].strip()

    return ""


def _build_experiment_table_text(experiments: list[Experiment]) -> str:
    """Build a text table of experiments for a slide."""
    lines = []
    for exp in experiments:
        date_str = exp.experiment_date.strftime("%m/%d") if exp.experiment_date else "?"
        runs = len(exp.runs)
        lines.append(
            f"{date_str} | {exp.purpose[:50]} | {exp.tester} | {exp.device} | {runs} runs"
        )
    return "\n".join(lines) if lines else "No experiments this week."


def _new_id() -> str:
    """Generate a unique object ID for Slides API."""
    return f"obj_{uuid4().hex[:12]}"
