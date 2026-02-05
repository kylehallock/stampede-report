"""Google Sheets API reader for experiment data."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SheetsReader:
    """Reads data from Google Sheets using the Sheets API v4."""

    def __init__(self, sheets_service):
        """Initialize with a Google Sheets API service instance.

        Args:
            sheets_service: google-api-python-client sheets service object.
        """
        self._service = sheets_service

    def read_sheet(
        self,
        spreadsheet_id: str,
        sheet_name: Optional[str] = None,
        range_str: Optional[str] = None,
    ) -> list[list[str]]:
        """Read all cell data from a Google Sheet.

        Args:
            spreadsheet_id: The Google Sheets file ID.
            sheet_name: Optional sheet tab name. Defaults to first sheet.
            range_str: Optional A1 range notation. Defaults to entire sheet.

        Returns:
            2D list of cell values as strings.
        """
        if range_str:
            full_range = f"{sheet_name}!{range_str}" if sheet_name else range_str
        elif sheet_name:
            full_range = sheet_name
        else:
            # Get the first sheet name
            spreadsheet = (
                self._service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
                .execute()
            )
            sheets = spreadsheet.get("sheets", [])
            if not sheets:
                return []
            full_range = sheets[0]["properties"]["title"]

        result = (
            self._service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=full_range,
                valueRenderOption="UNFORMATTED_VALUE",
                dateTimeRenderOption="FORMATTED_STRING",
            )
            .execute()
        )

        values = result.get("values", [])
        # Normalize: ensure all rows have the same number of columns
        if values:
            max_cols = max(len(row) for row in values)
            normalized = []
            for row in values:
                str_row = [str(cell) if cell is not None else "" for cell in row]
                # Pad short rows
                while len(str_row) < max_cols:
                    str_row.append("")
                normalized.append(str_row)
            return normalized

        return []

    def get_sheet_names(self, spreadsheet_id: str) -> list[str]:
        """Get all sheet tab names in a spreadsheet.

        Args:
            spreadsheet_id: The Google Sheets file ID.

        Returns:
            List of sheet tab names.
        """
        spreadsheet = (
            self._service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
            .execute()
        )
        return [
            sheet["properties"]["title"]
            for sheet in spreadsheet.get("sheets", [])
        ]
