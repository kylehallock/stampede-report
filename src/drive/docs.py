"""Google Docs API reader for journal and meeting minute documents."""

import logging

logger = logging.getLogger(__name__)


class DocsReader:
    """Reads content from Google Docs using the Docs API v1."""

    def __init__(self, docs_service):
        """Initialize with a Google Docs API service instance.

        Args:
            docs_service: google-api-python-client docs service object.
        """
        self._service = docs_service

    def read_document_text(self, document_id: str) -> str:
        """Extract plain text content from a Google Doc.

        Args:
            document_id: The Google Docs file ID.

        Returns:
            Plain text content of the document.
        """
        doc = self._service.documents().get(documentId=document_id).execute()
        body = doc.get("body", {})
        content = body.get("content", [])

        text_parts = []
        for element in content:
            self._extract_text(element, text_parts)

        return "".join(text_parts)

    def _extract_text(self, element: dict, parts: list[str]) -> None:
        """Recursively extract text from a document element."""
        if "paragraph" in element:
            paragraph = element["paragraph"]
            for elem in paragraph.get("elements", []):
                if "textRun" in elem:
                    parts.append(elem["textRun"].get("content", ""))
        elif "table" in element:
            table = element["table"]
            for row in table.get("tableRows", []):
                for cell in row.get("tableCells", []):
                    for content in cell.get("content", []):
                        self._extract_text(content, parts)
                    parts.append("\t")
                parts.append("\n")
        elif "sectionBreak" in element:
            pass  # Section breaks don't contain text
