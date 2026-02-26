"""
Tests for the Universal Document Ingester (services/document_ingester.py).

Covers:
  1. PDF text extraction (clear text + tables)
  2. PDF scanned detection (sparse text → likely_scanned)
  3. Image ingestion (JPEG → base64, resize ≤ 1568px)
  4. DOCX ingestion (paragraphs + tables)
  5. Excel non-standard sheets
  6. CSV Xero detected (is_structured=True)
  7. CSV non-standard (is_structured=False)
  8. Unsupported file format → UnsupportedFileTypeError
  9. Empty file → EmptyFileError
"""

import base64
import io
from pathlib import Path

import pytest

from services.document_ingester import (
    DocumentIngester,
    EmptyFileError,
    RawDocumentContent,
    UnsupportedFileTypeError,
)

FIXTURES = Path(__file__).parent / "fixtures"
ingester = DocumentIngester()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


# ---------------------------------------------------------------------------
# 1. PDF text extraction
# ---------------------------------------------------------------------------

class TestPdfTextExtraction:
    """PDF with clear text → text_content not empty, tables extracted."""

    def test_text_content_not_empty(self):
        result = ingester.ingest(_read("pbm_bank_statement.pdf"), "pbm_bank_statement.pdf", "application/pdf")
        assert result.file_type == "pdf"
        assert len(result.text_content.strip()) > 0

    def test_tables_extracted(self):
        result = ingester.ingest(_read("pbm_bank_statement.pdf"), "pbm_bank_statement.pdf", "application/pdf")
        assert len(result.tables) > 0
        # Each table should be a list of rows
        for table in result.tables:
            assert isinstance(table, list)
            for row in table:
                assert isinstance(row, list)

    def test_page_count_in_metadata(self):
        result = ingester.ingest(_read("pbm_bank_statement.pdf"), "pbm_bank_statement.pdf", "application/pdf")
        assert result.metadata["page_count"] >= 1

    def test_closing_balance_in_text(self):
        result = ingester.ingest(_read("pbm_bank_statement.pdf"), "pbm_bank_statement.pdf", "application/pdf")
        assert "59,689.27" in result.text_content


# ---------------------------------------------------------------------------
# 2. PDF scanned detection
# ---------------------------------------------------------------------------

class TestPdfScannedDetection:
    """Sparse PDF (image-only pages) → likely_scanned=True, images_base64 populated."""

    def test_likely_scanned_flag(self):
        result = ingester.ingest(_read("sparse_scanned.pdf"), "sparse_scanned.pdf", "application/pdf")
        assert result.likely_scanned is True

    def test_images_base64_populated(self):
        result = ingester.ingest(_read("sparse_scanned.pdf"), "sparse_scanned.pdf", "application/pdf")
        assert len(result.images_base64) > 0
        # Each entry should be valid base64
        for b64 in result.images_base64:
            decoded = base64.b64decode(b64)
            assert len(decoded) > 0


# ---------------------------------------------------------------------------
# 3. Image ingestion
# ---------------------------------------------------------------------------

class TestImageIngestion:
    """JPEG upload → file_type='image', images_base64 populated, ≤ 1568px."""

    def test_file_type(self):
        result = ingester.ingest(_read("scanned_placeholder.jpg"), "scanned_placeholder.jpg", "image/jpeg")
        assert result.file_type == "image"

    def test_images_base64_populated(self):
        result = ingester.ingest(_read("scanned_placeholder.jpg"), "scanned_placeholder.jpg", "image/jpeg")
        assert len(result.images_base64) == 1

    def test_image_dimensions_within_limit(self):
        from PIL import Image as PILImage

        result = ingester.ingest(_read("scanned_placeholder.jpg"), "scanned_placeholder.jpg", "image/jpeg")
        # Decode the base64 image and check dimensions
        img_bytes = base64.b64decode(result.images_base64[0])
        img = PILImage.open(io.BytesIO(img_bytes))
        assert max(img.size) <= 1568

    def test_large_image_is_resized(self):
        """An image larger than 1568px should be resized down."""
        from PIL import Image as PILImage

        # Create a 3000x2000 image
        big_img = PILImage.new("RGB", (3000, 2000), "blue")
        buf = io.BytesIO()
        big_img.save(buf, format="JPEG")
        big_bytes = buf.getvalue()

        result = ingester.ingest(big_bytes, "huge_photo.jpg", "image/jpeg")
        decoded = base64.b64decode(result.images_base64[0])
        resized = PILImage.open(io.BytesIO(decoded))
        assert max(resized.size) <= 1568


# ---------------------------------------------------------------------------
# 4. DOCX ingestion
# ---------------------------------------------------------------------------

class TestDocxIngestion:
    """Word doc → paragraphs and tables extracted, text_content not empty."""

    def test_text_content_not_empty(self):
        result = ingester.ingest(_read("pbm_balance_sheet.docx"), "pbm_balance_sheet.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        assert result.file_type == "docx"
        assert len(result.text_content.strip()) > 0

    def test_tables_extracted(self):
        result = ingester.ingest(_read("pbm_balance_sheet.docx"), "pbm_balance_sheet.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        assert len(result.tables) > 0

    def test_heading_preserved(self):
        result = ingester.ingest(_read("pbm_balance_sheet.docx"), "pbm_balance_sheet.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        assert "PBM" in result.text_content

    def test_document_properties(self):
        result = ingester.ingest(_read("pbm_balance_sheet.docx"), "pbm_balance_sheet.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        assert "title" in result.metadata
        assert "author" in result.metadata


# ---------------------------------------------------------------------------
# 5. Excel non-standard sheets
# ---------------------------------------------------------------------------

class TestExcelNonstandardSheets:
    """Excel with unusual columns → all sheets returned, is_structured=False."""

    def test_all_sheets_returned(self):
        result = ingester.ingest(_read("nonstandard_excel.xlsx"), "nonstandard_excel.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        assert result.file_type == "excel"
        assert result.metadata["sheet_count"] == 2
        assert "Inventory" in result.metadata["sheet_names"]
        assert "Payroll" in result.metadata["sheet_names"]

    def test_tables_for_each_sheet(self):
        result = ingester.ingest(_read("nonstandard_excel.xlsx"), "nonstandard_excel.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        assert len(result.tables) == 2

    def test_is_structured_false(self):
        result = ingester.ingest(_read("nonstandard_excel.xlsx"), "nonstandard_excel.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        assert result.is_structured is False


# ---------------------------------------------------------------------------
# 6. CSV — Xero detected
# ---------------------------------------------------------------------------

class TestCsvXeroDetected:
    """Xero CSV (pbm_aged_payables.csv) → is_structured=True."""

    def test_is_structured_true(self):
        result = ingester.ingest(_read("pbm_aged_payables.csv"), "pbm_aged_payables.csv", "text/csv")
        assert result.file_type == "csv"
        assert result.is_structured is True

    def test_table_populated(self):
        result = ingester.ingest(_read("pbm_aged_payables.csv"), "pbm_aged_payables.csv", "text/csv")
        assert len(result.tables) == 1
        # Header + 6 creditor rows
        assert len(result.tables[0]) >= 7

    def test_columns_in_metadata(self):
        result = ingester.ingest(_read("pbm_aged_payables.csv"), "pbm_aged_payables.csv", "text/csv")
        assert "Contact" in result.metadata["columns"]


# ---------------------------------------------------------------------------
# 7. CSV — non-standard
# ---------------------------------------------------------------------------

class TestCsvNonstandard:
    """CSV with unknown columns → is_structured=False."""

    def test_is_structured_false(self):
        result = ingester.ingest(_read("nonstandard.csv"), "nonstandard.csv", "text/csv")
        assert result.file_type == "csv"
        assert result.is_structured is False

    def test_text_content_populated(self):
        result = ingester.ingest(_read("nonstandard.csv"), "nonstandard.csv", "text/csv")
        assert "Widget" in result.text_content


# ---------------------------------------------------------------------------
# 8. Unsupported file format
# ---------------------------------------------------------------------------

class TestUnsupportedFormat:
    """.exe upload → raises UnsupportedFileTypeError."""

    def test_raises_error(self):
        with pytest.raises(UnsupportedFileTypeError):
            ingester.ingest(b"MZ\x90\x00", "malware.exe", "application/octet-stream")

    def test_error_message_includes_extension(self):
        with pytest.raises(UnsupportedFileTypeError, match=r"\.exe"):
            ingester.ingest(b"MZ\x90\x00", "malware.exe", "application/octet-stream")


# ---------------------------------------------------------------------------
# 9. Empty file
# ---------------------------------------------------------------------------

class TestEmptyFile:
    """Zero-byte file → raises EmptyFileError."""

    def test_raises_error(self):
        with pytest.raises(EmptyFileError):
            ingester.ingest(b"", "nothing.pdf", "application/pdf")

    def test_error_message_includes_filename(self):
        with pytest.raises(EmptyFileError, match="nothing.pdf"):
            ingester.ingest(b"", "nothing.pdf", "application/pdf")
