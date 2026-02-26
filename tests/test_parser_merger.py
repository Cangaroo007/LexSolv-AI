"""Tests for the parser merger (Prompt 5.2 — Task D).

Tests the merge logic between structured parser and AI parser results.
"""

from __future__ import annotations

import pytest

from services.ai_parser import AIParseResult
from services.document_ingester import RawDocumentContent
from services.parser_merger import ConflictRecord, MergedParseResult, ParserMerger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw(is_structured: bool = False) -> RawDocumentContent:
    """Build a minimal RawDocumentContent for testing."""
    return RawDocumentContent(
        filename="test.csv",
        file_type="csv",
        text_content="test content",
        tables=[],
        images_base64=[],
        metadata={},
        likely_scanned=False,
        is_structured=is_structured,
        raw_bytes=b"dummy",
    )


def _make_ai_result(
    extracted: dict,
    confidence: dict,
    document_type: str = "aged_payables",
    notes: list | None = None,
) -> AIParseResult:
    return AIParseResult(
        document_type=document_type,
        extracted=extracted,
        confidence=confidence,
        notes=notes or [],
        parse_method="ai_text",
        tokens_used=500,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParserMerger:

    def test_merger_structured_wins(self):
        """Both parsers have a value, structured confidence >= 0.95 → structured value used."""
        merger = ParserMerger()
        raw = _make_raw(is_structured=True)

        structured = {"closing_balance": 42000.0, "period_end_date": "2025-12-31"}
        ai_result = _make_ai_result(
            extracted={"closing_balance": 42000.0, "period_end_date": "2025-12-31"},
            confidence={"closing_balance": 0.9, "period_end_date": 0.85},
            document_type="bank_statement",
        )

        merged = merger.merge(structured, ai_result, raw)

        assert isinstance(merged, MergedParseResult)
        assert merged.fields["closing_balance"] == 42000.0
        # Structured wins because is_structured=True and default conf (0.98) >= 0.95
        assert merged.source["closing_balance"] == "structured_parser"
        assert merged.confidence["closing_balance"] == pytest.approx(0.98, abs=0.05)

    def test_merger_ai_fills_gap(self):
        """Structured parser missing a field → AI value used, source='ai_parser'."""
        merger = ParserMerger()
        raw = _make_raw(is_structured=True)

        # Structured only has closing_balance
        structured = {"closing_balance": 42000.0}
        # AI has both closing_balance AND period_end_date
        ai_result = _make_ai_result(
            extracted={"closing_balance": 42000.0, "period_end_date": "2025-12-31"},
            confidence={"closing_balance": 0.95, "period_end_date": 0.85},
            document_type="bank_statement",
        )

        merged = merger.merge(structured, ai_result, raw)

        # period_end_date should come from AI since structured doesn't have it
        assert merged.fields["period_end_date"] == "2025-12-31"
        assert merged.source["period_end_date"] == "ai_parser"
        assert merged.confidence["period_end_date"] == pytest.approx(0.85)
        # Summary should mention AI-filled
        assert "AI-filled" in merged.parse_summary

    def test_merger_conflict_flagged(self):
        """Values differ by >5% → ConflictRecord in conflicts list."""
        merger = ParserMerger()
        raw = _make_raw(is_structured=True)

        # Structured says 100,000; AI says 120,000 → 20% difference
        structured = {"total_liabilities": 100000.0}
        ai_result = _make_ai_result(
            extracted={"total_liabilities": 120000.0},
            confidence={"total_liabilities": 0.85},
            document_type="balance_sheet",
        )

        merged = merger.merge(structured, ai_result, raw)

        assert len(merged.conflicts) == 1
        conflict = merged.conflicts[0]
        assert isinstance(conflict, ConflictRecord)
        assert conflict.field == "total_liabilities"
        assert conflict.structured_value == 100000.0
        assert conflict.ai_value == 120000.0
        assert merged.source["total_liabilities"] == "conflict"
        # Since is_structured and structured conf >= 0.95, structured wins
        assert merged.fields["total_liabilities"] == 100000.0

    def test_merger_structured_only(self):
        """ai_result=None → structured result returned with source tags."""
        merger = ParserMerger()
        raw = _make_raw(is_structured=True)

        structured = {
            "closing_balance": 50000.0,
            "period_end_date": "2025-06-30",
        }

        merged = merger.merge(structured, None, raw)

        assert merged.fields["closing_balance"] == 50000.0
        assert merged.fields["period_end_date"] == "2025-06-30"
        assert merged.source["closing_balance"] == "structured_parser"
        assert merged.source["period_end_date"] == "structured_parser"
        assert len(merged.conflicts) == 0
        assert "structured parser only" in merged.parse_summary.lower()

    def test_merger_ai_only(self):
        """structured=None → AI result returned with source tags."""
        merger = ParserMerger()
        raw = _make_raw(is_structured=False)

        ai_result = _make_ai_result(
            extracted={
                "creditors": [
                    {"name": "ATO", "amount": 150000.0},
                    {"name": "Telstra", "amount": 2500.0},
                ],
            },
            confidence={"creditors": 0.9},
            document_type="aged_payables",
        )

        merged = merger.merge(None, ai_result, raw)

        assert "creditors" in merged.fields
        assert len(merged.fields["creditors"]) == 2
        assert merged.source["creditors"] == "ai_parser"
        assert merged.confidence["creditors"] == pytest.approx(0.9)
        assert len(merged.conflicts) == 0
        assert "ai" in merged.parse_summary.lower()
