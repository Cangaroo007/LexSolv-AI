"""Tests for the AI parser (Prompt 5.2 — Task D).

All tests mock ClaudeClient — no live API calls, no ANTHROPIC_API_KEY needed.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.ai_parser import AIParser, AIParseResult, DOCUMENT_SCHEMAS
from services.claude_client import ClaudeClient, GenerateResult
from services.document_ingester import RawDocumentContent

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw(
    *,
    file_type: str = "pdf",
    text_content: str = "",
    tables: list | None = None,
    images_base64: list | None = None,
    likely_scanned: bool = False,
    is_structured: bool = False,
) -> RawDocumentContent:
    """Build a minimal RawDocumentContent for testing."""
    return RawDocumentContent(
        filename=f"test.{file_type}",
        file_type=file_type,
        text_content=text_content,
        tables=tables or [],
        images_base64=images_base64 or [],
        metadata={},
        likely_scanned=likely_scanned,
        is_structured=is_structured,
        raw_bytes=b"dummy",
    )


def _mock_claude_response(fields: dict, confidence: dict, notes: list | None = None) -> GenerateResult:
    """Build a GenerateResult whose .text is valid JSON matching Claude's expected format."""
    payload = {
        "fields": fields,
        "confidence": confidence,
        "notes": notes or [],
    }
    return GenerateResult(
        text=json.dumps(payload),
        input_tokens=500,
        output_tokens=300,
        model="claude-sonnet-4-20250514",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ai_parser_aged_payables_pdf():
    """PDF input → correct creditor structure returned."""
    mock_result = _mock_claude_response(
        fields={
            "creditors": [
                {"name": "ATO", "amount": 150000.0, "category": "ato_other", "is_related_party": False, "is_disputed": False},
                {"name": "Telstra", "amount": 2500.50, "category": "trade", "is_related_party": False, "is_disputed": False},
            ],
        },
        confidence={
            "creditors": 0.95,
            "creditors[0].amount": 1.0,
            "creditors[1].amount": 0.95,
        },
        notes=["Two creditors identified from aged payables table"],
    )

    raw = _make_raw(
        file_type="pdf",
        text_content="Aged Payables Report\nATO  $150,000.00\nTelstra  $2,500.50",
        tables=[[["Creditor", "Amount"], ["ATO", "150000"], ["Telstra", "2500.50"]]],
    )

    with patch.object(ClaudeClient, "generate", new_callable=AsyncMock, return_value=mock_result):
        parser = AIParser()
        result = await parser.parse(raw, "aged_payables", "eng-001")

    assert isinstance(result, AIParseResult)
    assert result.document_type == "aged_payables"
    assert "creditors" in result.extracted
    assert len(result.extracted["creditors"]) == 2
    assert result.extracted["creditors"][0]["amount"] == 150000.0
    assert result.parse_method == "ai_text"
    assert result.tokens_used == 800


@pytest.mark.asyncio
async def test_ai_parser_balance_sheet_image():
    """Image input → vision message constructed, assets extracted."""
    mock_result = _mock_claude_response(
        fields={
            "assets": [
                {"name": "Cash at Bank", "asset_type": "cash", "book_value": 50000.0, "recovery_pct": 100.0},
                {"name": "Motor Vehicles", "asset_type": "motor_vehicles", "book_value": 25000.0, "recovery_pct": 60.0},
            ],
            "total_liabilities": 200000.0,
        },
        confidence={
            "assets": 0.85,
            "total_liabilities": 0.9,
        },
    )

    raw = _make_raw(
        file_type="image",
        text_content="",
        images_base64=["base64encodedimage=="],
        likely_scanned=False,  # image file_type triggers vision path
    )

    with patch.object(ClaudeClient, "generate", new_callable=AsyncMock, return_value=mock_result) as mock_gen:
        parser = AIParser()
        result = await parser.parse(raw, "balance_sheet", "eng-002")

    assert result.parse_method == "ai_vision"
    assert "assets" in result.extracted
    assert len(result.extracted["assets"]) == 2
    assert result.extracted["total_liabilities"] == 200000.0

    # Verify vision path was used — images_base64 reference should be in the prompt
    call_args = mock_gen.call_args
    user_prompt = call_args.kwargs.get("user_prompt") or call_args[1] if len(call_args) > 1 else call_args.kwargs.get("user_prompt", "")
    if isinstance(user_prompt, str):
        assert "image(s)" in user_prompt.lower() or "image" in user_prompt.lower()


@pytest.mark.asyncio
async def test_ai_parser_missing_fields_omitted():
    """Mock Claude omits a field → field absent from AIParseResult (not null, not zero)."""
    # Claude response omits "net_profit" — only returns total_revenue
    mock_result = _mock_claude_response(
        fields={"total_revenue": 500000.0},
        confidence={"total_revenue": 1.0},
        notes=["Net profit not found in document"],
    )

    raw = _make_raw(
        file_type="pdf",
        text_content="Revenue: $500,000",
    )

    with patch.object(ClaudeClient, "generate", new_callable=AsyncMock, return_value=mock_result):
        parser = AIParser()
        result = await parser.parse(raw, "pnl", "eng-003")

    assert "total_revenue" in result.extracted
    assert result.extracted["total_revenue"] == 500000.0
    # net_profit was not in Claude's response — must NOT appear
    assert "net_profit" not in result.extracted
    assert "gross_profit" not in result.extracted


@pytest.mark.asyncio
async def test_ai_parser_pii_scrubbed():
    """Verify PrivacyVault.scrub() called before Claude, .restore() called after."""
    mock_result = _mock_claude_response(
        fields={"closing_balance": 42000.0, "period_end_date": "2025-12-31"},
        confidence={"closing_balance": 1.0, "period_end_date": 1.0},
    )

    raw = _make_raw(
        file_type="pdf",
        text_content="Bank Statement for Dr James Mitchell\nClosing Balance: $42,000",
    )

    with patch("services.ai_parser.scrub", wraps=__import__("services.privacy_vault", fromlist=["scrub"]).scrub) as mock_scrub, \
         patch("services.ai_parser.restore", wraps=__import__("services.privacy_vault", fromlist=["restore"]).restore) as mock_restore, \
         patch.object(ClaudeClient, "generate", new_callable=AsyncMock, return_value=mock_result):

        parser = AIParser()
        result = await parser.parse(
            raw, "bank_statement", "eng-004",
            known_entities={"client_name": ["Dr James Mitchell"]},
        )

    # scrub() must be called BEFORE Claude (which happens inside parse)
    assert mock_scrub.called, "PrivacyVault.scrub() was not called before Claude"
    # restore() must be called AFTER Claude returns
    assert mock_restore.called, "PrivacyVault.restore() was not called after Claude"

    assert result.extracted["closing_balance"] == 42000.0


@pytest.mark.asyncio
async def test_ai_parser_invalid_json_handled():
    """Mock Claude returns non-JSON → graceful error, not 500."""
    bad_result = GenerateResult(
        text="Sorry, I couldn't parse the document. It appears to be corrupted.",
        input_tokens=400,
        output_tokens=20,
        model="claude-sonnet-4-20250514",
    )

    raw = _make_raw(file_type="pdf", text_content="corrupted content")

    with patch.object(ClaudeClient, "generate", new_callable=AsyncMock, return_value=bad_result):
        parser = AIParser()
        result = await parser.parse(raw, "aged_payables", "eng-005")

    # Should not raise — returns an AIParseResult with empty extracted
    assert isinstance(result, AIParseResult)
    assert result.extracted == {}
    assert result.confidence == {}
    assert len(result.notes) > 0
    assert "invalid" in result.notes[0].lower() or "failed" in result.notes[0].lower()
    assert result.tokens_used == 420
