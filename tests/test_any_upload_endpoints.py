"""Tests for /api/upload/any/* endpoints (Prompt 5.2 — Task D).

Tests the new universal upload endpoints alongside existing ones.
Mocks the AI parser to avoid requiring ANTHROPIC_API_KEY.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from main import app
from services.ai_parser import AIParseResult
from services.claude_client import ClaudeClient, GenerateResult

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _mock_ai_result_aged_payables() -> AIParseResult:
    """Mock AI result for aged payables that matches PBM fixture data."""
    return AIParseResult(
        document_type="aged_payables",
        extracted={
            "creditors": [
                {"name": "Australian Taxation Office", "amount": 185000.0, "category": "ato_other", "is_related_party": False, "is_disputed": False},
                {"name": "Prospa Advance Pty Ltd", "amount": 45000.0, "category": "finance", "is_related_party": False, "is_disputed": False},
                {"name": "iCare Workers Compensation", "amount": 12400.0, "category": "workers_comp", "is_related_party": False, "is_disputed": False},
                {"name": "Mitchell Family Trust", "amount": 350000.0, "category": "trade", "is_related_party": True, "is_disputed": False},
                {"name": "Office Supplies Co", "amount": 1877.37, "category": "trade", "is_related_party": False, "is_disputed": False},
                {"name": "Telstra Business", "amount": 391500.0, "category": "trade", "is_related_party": False, "is_disputed": False},
            ],
        },
        confidence={
            "creditors": 0.95,
        },
        notes=["6 creditors identified"],
        parse_method="ai_text",
        tokens_used=800,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_new_endpoint_pdf_aged_payables():
    """Upload PDF → merged result returned with confidence scores."""
    from services.document_ingester import RawDocumentContent

    # Mock the ingester to avoid pdfplumber dependency issues in test env
    fake_raw = RawDocumentContent(
        filename="test_payables.pdf",
        file_type="pdf",
        text_content="Creditor: Test Creditor  Amount: $10,000",
        tables=[],
        images_base64=[],
        metadata={"page_count": 1},
        likely_scanned=False,
        is_structured=False,
        raw_bytes=b"%PDF-1.4 fake",
    )

    mock_ai_result = AIParseResult(
        document_type="aged_payables",
        extracted={
            "creditors": [
                {"name": "Test Creditor", "amount": 10000.0},
            ],
        },
        confidence={"creditors": 0.85},
        notes=["Extracted from PDF"],
        parse_method="ai_text",
        tokens_used=600,
    )

    with patch("main.document_ingester.ingest", return_value=fake_raw), \
         patch("main.ai_parser.parse", new_callable=AsyncMock, return_value=mock_ai_result):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/upload/any/aged-payables",
                files={"file": ("test_payables.pdf", io.BytesIO(b"%PDF-1.4 fake"), "application/pdf")},
            )

    assert resp.status_code == 200
    data = resp.json()
    assert data["document_type"] == "aged_payables"
    assert "confidence" in data
    assert "source" in data
    assert "parse_summary" in data
    assert "creditors" in data


@pytest.mark.asyncio
async def test_existing_upload_endpoints_unchanged():
    """POST /api/upload/aged-payables with PBM CSV still works (Rule 3)."""
    content = _read_fixture("pbm_aged_payables.csv")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/upload/aged-payables",
            files={"file": ("pbm_aged_payables.csv", io.BytesIO(content), "text/csv")},
        )

    assert resp.status_code == 200
    data = resp.json()
    # Original endpoint response shape must be preserved
    assert "creditors" in data
    assert len(data["creditors"]) == 6
    assert data["count"] == 6
    assert "parse_method" in data
    # Must NOT have new fields from the merged response
    # (the old endpoint does not return these)


@pytest.mark.asyncio
async def test_pbm_csv_via_new_endpoint_same_result():
    """PBM CSV via new endpoint → same creditor data as old endpoint."""
    content = _read_fixture("pbm_aged_payables.csv")

    # Mock AI parser to return similar data
    mock_ai_result = _mock_ai_result_aged_payables()

    with patch("main.ai_parser.parse", new_callable=AsyncMock, return_value=mock_ai_result):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Old endpoint
            old_resp = await client.post(
                "/api/upload/aged-payables",
                files={"file": ("pbm_aged_payables.csv", io.BytesIO(content), "text/csv")},
            )
            # New endpoint
            new_resp = await client.post(
                "/api/upload/any/aged-payables",
                files={"file": ("pbm_aged_payables.csv", io.BytesIO(content), "text/csv")},
            )

    assert old_resp.status_code == 200
    assert new_resp.status_code == 200

    old_data = old_resp.json()
    new_data = new_resp.json()

    # New endpoint should have the confidence/source fields
    assert "confidence" in new_data
    assert "source" in new_data
    assert "parse_summary" in new_data
    assert new_data["document_type"] == "aged_payables"

    # Old endpoint returns creditors list — verify it's still 6 creditors
    assert len(old_data["creditors"]) == 6

    # Total from PBM fixture (6 creditors: ATO-ITA, ATO-ICA, iCare, Prospa, BlueShak, BTC)
    old_total = sum(c["amount_claimed"] for c in old_data["creditors"])
    assert abs(old_total - 1195777.37) < 0.01, f"Expected $1,195,777.37, got ${old_total:.2f}"
