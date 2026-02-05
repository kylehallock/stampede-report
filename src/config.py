"""Configuration settings for the Stampede Weekly Report Generator."""

import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
KNOWLEDGE_DIR = DATA_DIR / "institutional_knowledge"
CUMULATIVE_LEARNINGS_PATH = DATA_DIR / "cumulative_learnings.json"
CHARTS_DIR = DATA_DIR / "charts"

# Google API settings
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/documents.readonly",
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive.file",
]

DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "")
REPORTS_FOLDER_ID = os.getenv("REPORTS_FOLDER_ID", "")

# Anthropic API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"

# Bootstrap half-year periods
HALF_YEAR_PERIODS = [
    ("H1_2022", "2022-01-01", "2022-06-30"),
    ("H2_2022", "2022-07-01", "2022-12-31"),
    ("H1_2023", "2023-01-01", "2023-06-30"),
    ("H2_2023", "2023-07-01", "2023-12-31"),
    ("H1_2024", "2024-01-01", "2024-06-30"),
    ("H2_2024", "2024-07-01", "2024-12-31"),
    ("H1_2025", "2025-01-01", "2025-06-30"),
    ("H2_2025", "2025-07-01", "2025-12-31"),
]

EXPERIMENT_BATCH_SIZE = 12


def get_google_credentials_info() -> dict:
    """Load Google service account credentials from env var.

    The env var can be either a file path to a JSON key or the JSON content itself.
    """
    key_data = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY", "")
    if not key_data:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY environment variable not set")

    # Check if it's a file path
    if os.path.isfile(key_data):
        with open(key_data) as f:
            return json.load(f)

    # Otherwise treat as raw JSON
    return json.loads(key_data)
