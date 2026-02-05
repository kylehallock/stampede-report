"""Google Drive API client for file discovery and authentication."""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.config import GOOGLE_SCOPES, get_google_credentials_info

logger = logging.getLogger(__name__)

# MIME types for Google Workspace files
MIME_SPREADSHEET = "application/vnd.google-apps.spreadsheet"
MIME_DOCUMENT = "application/vnd.google-apps.document"
MIME_PRESENTATION = "application/vnd.google-apps.presentation"
MIME_FOLDER = "application/vnd.google-apps.folder"


class DriveClient:
    """Google Drive API client for file discovery and authentication."""

    def __init__(self, credentials_info: Optional[dict] = None):
        """Initialize the Drive client.

        Args:
            credentials_info: Service account credentials dict. If None,
                loads from environment.
        """
        if credentials_info is None:
            credentials_info = get_google_credentials_info()

        self._credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=GOOGLE_SCOPES
        )
        self._drive_service = build("drive", "v3", credentials=self._credentials)
        self._sheets_service = build("sheets", "v4", credentials=self._credentials)
        self._docs_service = build("docs", "v1", credentials=self._credentials)
        self._slides_service = build("slides", "v1", credentials=self._credentials)

    def _is_shared_drive(self, folder_id: str) -> bool:
        """Check if a folder ID is a Shared Drive (Team Drive)."""
        try:
            # Try to get it as a shared drive
            self._drive_service.drives().get(driveId=folder_id).execute()
            return True
        except Exception:
            return False

    @property
    def credentials(self):
        return self._credentials

    @property
    def drive(self):
        return self._drive_service

    @property
    def sheets(self):
        return self._sheets_service

    @property
    def docs(self):
        return self._docs_service

    @property
    def slides(self):
        return self._slides_service

    def list_files_in_folder(
        self,
        folder_id: str,
        mime_type: Optional[str] = None,
        recursive: bool = True,
    ) -> list[dict]:
        """List all files in a Drive folder.

        Args:
            folder_id: Google Drive folder ID (supports Shared Drives).
            mime_type: Optional MIME type filter.
            recursive: Whether to recurse into subfolders.

        Returns:
            List of file metadata dicts with id, name, mimeType, modifiedTime.
        """
        all_files = []
        is_shared_drive = self._is_shared_drive(folder_id)
        self._list_files_recursive(folder_id, mime_type, recursive, all_files, is_shared_drive, folder_id if is_shared_drive else None)
        return all_files

    def _list_files_recursive(
        self,
        folder_id: str,
        mime_type: Optional[str],
        recursive: bool,
        results: list[dict],
        is_shared_drive: bool = False,
        drive_id: Optional[str] = None,
    ) -> None:
        """Recursively list files in a folder."""
        query_parts = [f"'{folder_id}' in parents", "trashed = false"]
        if mime_type:
            query_parts.append(f"mimeType = '{mime_type}'")

        query = " and ".join(query_parts)
        page_token = None

        while True:
            list_params = {
                "q": query,
                "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime)",
                "pageToken": page_token,
                "pageSize": 100,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }
            # For Shared Drives, use corpora='drive' with driveId
            if is_shared_drive and drive_id:
                list_params["corpora"] = "drive"
                list_params["driveId"] = drive_id
            else:
                list_params["spaces"] = "drive"

            response = self._drive_service.files().list(**list_params).execute()

            files = response.get("files", [])
            results.extend(files)
            page_token = response.get("nextPageToken")
            if not page_token:
                break

        # Recurse into subfolders
        if recursive:
            folder_query = (
                f"'{folder_id}' in parents and "
                f"mimeType = '{MIME_FOLDER}' and "
                "trashed = false"
            )
            page_token = None
            while True:
                list_params = {
                    "q": folder_query,
                    "fields": "nextPageToken, files(id, name)",
                    "pageToken": page_token,
                    "pageSize": 100,
                    "supportsAllDrives": True,
                    "includeItemsFromAllDrives": True,
                }
                if is_shared_drive and drive_id:
                    list_params["corpora"] = "drive"
                    list_params["driveId"] = drive_id
                else:
                    list_params["spaces"] = "drive"

                response = self._drive_service.files().list(**list_params).execute()
                folders = response.get("files", [])
                for folder in folders:
                    self._list_files_recursive(
                        folder["id"], mime_type, recursive, results, is_shared_drive, drive_id
                    )
                page_token = response.get("nextPageToken")
                if not page_token:
                    break

    def list_recent_files(
        self,
        folder_id: str,
        days: int = 7,
        mime_type: Optional[str] = None,
    ) -> list[dict]:
        """List files modified in the last N days.

        Uses direct API query with modifiedTime filter for efficiency.
        Much faster than recursive traversal for large Shared Drives.

        Args:
            folder_id: Google Drive folder ID or Shared Drive ID.
            days: Number of days to look back.
            mime_type: Optional MIME type filter.

        Returns:
            List of recently modified file metadata dicts.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S")

        # Build query with modification time filter
        query_parts = [f"modifiedTime > '{cutoff_str}'", "trashed = false"]
        if mime_type:
            query_parts.append(f"mimeType = '{mime_type}'")
        query = " and ".join(query_parts)

        is_shared_drive = self._is_shared_drive(folder_id)
        results = []
        page_token = None

        while True:
            list_params = {
                "q": query,
                "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime)",
                "pageToken": page_token,
                "pageSize": 100,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }

            if is_shared_drive:
                list_params["corpora"] = "drive"
                list_params["driveId"] = folder_id
            else:
                list_params["spaces"] = "drive"

            response = self._drive_service.files().list(**list_params).execute()
            files = response.get("files", [])
            results.extend(files)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return results

    def list_spreadsheets(self, folder_id: str) -> list[dict]:
        """List all Google Sheets in a folder."""
        return self.list_files_in_folder(folder_id, MIME_SPREADSHEET)

    def list_documents(self, folder_id: str) -> list[dict]:
        """List all Google Docs in a folder."""
        return self.list_files_in_folder(folder_id, MIME_DOCUMENT)

    def get_file_metadata(self, file_id: str) -> dict:
        """Get metadata for a specific file."""
        return (
            self._drive_service.files()
            .get(fileId=file_id, fields="id, name, mimeType, modifiedTime, createdTime")
            .execute()
        )

    def upload_file(
        self,
        file_path: str,
        folder_id: str,
        mime_type: str,
        name: Optional[str] = None,
    ) -> str:
        """Upload a file to Google Drive.

        Args:
            file_path: Local file path.
            folder_id: Destination folder ID (supports Shared Drives).
            mime_type: MIME type of the file.
            name: Optional name override.

        Returns:
            File ID of the uploaded file.
        """
        from googleapiclient.http import MediaFileUpload

        file_metadata = {
            "name": name or os.path.basename(file_path),
            "parents": [folder_id],
        }
        media = MediaFileUpload(file_path, mimetype=mime_type)
        file = (
            self._drive_service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        return file.get("id", "")

    def create_presentation_in_folder(
        self,
        title: str,
        folder_id: str,
    ) -> tuple[str, dict]:
        """Create a Google Slides presentation directly in a folder.

        For Shared Drives, creates the file with the folder as parent.
        This avoids permission issues with move operations.

        Args:
            title: Presentation title.
            folder_id: Destination folder ID.

        Returns:
            Tuple of (presentation_id, presentation_object).
        """
        # Create file metadata with target folder as parent
        file_metadata = {
            "name": title,
            "mimeType": MIME_PRESENTATION,
            "parents": [folder_id],
        }

        # Create the file using Drive API (not Slides API) to set parent
        file = (
            self._drive_service.files()
            .create(
                body=file_metadata,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        presentation_id = file["id"]

        # Get the full presentation object via Slides API
        presentation = (
            self._slides_service.presentations()
            .get(presentationId=presentation_id)
            .execute()
        )

        return presentation_id, presentation
