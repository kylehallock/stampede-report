# Stampede Weekly Report Generator

Automated weekly report pipeline for the Stampede TB diagnostics R&D project. Ingests experiment data and journals from Google Drive, analyzes results using Claude AI, generates charts, and produces a Google Slides report.

## Modes

- **Bootstrap** (one-time): Processes 4 years of historical data to build institutional knowledge
- **Weekly Report** (recurring): Analyzes the latest week's experiments and generates a report

## Setup

### 1. Google Cloud
1. Enable Drive, Sheets, Docs, and Slides APIs in your GCP project
2. Create a service account and download the JSON key
3. Share your Stampede Drive folder with the service account email

### 2. Environment Variables
```
GOOGLE_SERVICE_ACCOUNT_KEY=path/to/key.json  # or raw JSON
ANTHROPIC_API_KEY=sk-ant-...
DRIVE_FOLDER_ID=your-drive-folder-id
REPORTS_FOLDER_ID=your-reports-folder-id
```

### 3. Install
```bash
pip install -r requirements.txt
```

## Usage

### Weekly Report
```bash
python -m src.main                    # Default: last 7 days
python -m src.main --days=14          # Custom lookback
python -m src.main --dry-run          # Analysis only, no Slides
```

### Bootstrap (one-time)
```bash
python -m src.bootstrap.knowledge_builder
```

## GitHub Actions

The workflow runs automatically every Monday at 7 AM UTC, or can be triggered manually. Set the following repository secrets:
- `GOOGLE_SERVICE_ACCOUNT_KEY`
- `ANTHROPIC_API_KEY`
- `DRIVE_FOLDER_ID`
- `REPORTS_FOLDER_ID`

