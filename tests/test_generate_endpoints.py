"""Tests for /api/engagements/{id}/generate/* document generation endpoints.

All tests mock the database — no real DB or Claude calls.
Uses the same test client and DB mock patterns from test_narrative_api.py.
"""

from __future__ import annotations

import os
import uuid
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from main import app


# ---------------------------------------------------------------------------
#  Shared fixtures and helpers
# ---------------------------------------------------------------------------

_TEST_COMPANY_ID = uuid.uuid4()


class _FakeCompany:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.legal_name = kwargs.get("legal_name", "Test Co Pty Ltd")
        self.acn = kwargs.get("acn", "123456789")
        self.abn = kwargs.get("abn")
        self.total_creditors = kwargs.get("total_creditors", 985_777.37)
        self.custom_glossary = kwargs.get("custom_glossary", None)
        self.created_at = "2026-01-01T00:00:00"


class _FakePlan:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.company_id = kwargs.get("company_id")
        self.total_contribution = kwargs.get("total_contribution", 516_000.0)
        self.practitioner_fee_pct = kwargs.get("practitioner_fee_pct", 10.0)
        self.num_initial_payments = kwargs.get("num_initial_payments", 2)
        self.initial_payment_amount = kwargs.get("initial_payment_amount", 32_500.0)
        self.num_ongoing_payments = kwargs.get("num_ongoing_payments", 22)
        self.ongoing_payment_amount = kwargs.get("ongoing_payment_amount", 20_500.0)
        self.est_liquidator_fees = kwargs.get("est_liquidator_fees", 50_000.0)
        self.est_legal_fees = kwargs.get("est_legal_fees", 10_000.0)
        self.est_disbursements = kwargs.get("est_disbursements", 5_000.0)


class _FakeAsset:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.company_id = kwargs.get("company_id")
        self.asset_type = kwargs.get("asset_type", "cash")
        self.description = kwargs.get("description", "Cash at Bank")
        self.book_value = kwargs.get("book_value", 59_689.27)
        self.liquidation_recovery_pct = kwargs.get("liquidation_recovery_pct", 0.20)
        self.liquidation_value = kwargs.get("liquidation_value", 11_937.85)


class _FakeCreditor:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.company_id = kwargs.get("company_id")
        self.creditor_name = kwargs.get("creditor_name", "Supplier Co")
        self.amount_claimed = kwargs.get("amount_claimed", 50_000.0)
        self.category = kwargs.get("category", "unsecured")
        self.status = kwargs.get("status", "active")
        self.is_related_party = kwargs.get("is_related_party", False)
        self.is_secured = kwargs.get("is_secured", False)
        self.can_vote = kwargs.get("can_vote", True)
        self.notes = kwargs.get("notes")


class _FakeNarrative:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.engagement_id = kwargs.get("engagement_id", _TEST_COMPANY_ID)
        self.section = kwargs.get("section", "background")
        self.content = kwargs.get("content", "Generated content.")
        self.status = kwargs.get("status", "draft")
        self.metadata_ = kwargs.get("metadata_", {})
        self.entity_map = kwargs.get("entity_map", {})


class _FakeScalarResult:
    """Mimics SQLAlchemy scalar result."""

    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def scalars(self):
        return self

    def all(self):
        if isinstance(self._value, list):
            return self._value
        return [self._value] if self._value else []


def _build_mock_db(results_in_order: list):
    """Build an AsyncMock db session returning controlled query results."""
    mock_db = AsyncMock()
    _call_count = {"n": 0}

    async def _execute(stmt):
        idx = _call_count["n"]
        _call_count["n"] += 1
        if idx < len(results_in_order):
            return _FakeScalarResult(results_in_order[idx])
        return _FakeScalarResult(None)

    mock_db.execute = _execute
    mock_db.flush = AsyncMock()
    mock_db.commit = AsyncMock()
    mock_db.rollback = AsyncMock()
    mock_db.close = AsyncMock()
    mock_db.add = lambda x: None
    return mock_db


def _override_db(mock_db):
    """Return a dependency override generator for get_db."""
    from db.database import get_db as _original_get_db

    async def _get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _get_db
    return _original_get_db


# Standard test data
_company = _FakeCompany(id=_TEST_COMPANY_ID)
_plan = _FakePlan(company_id=_TEST_COMPANY_ID)
_assets = [
    _FakeAsset(asset_type="cash", description="Cash at Bank", book_value=59_689.27, liquidation_recovery_pct=0.20, liquidation_value=11_937.85),
    _FakeAsset(asset_type="receivables", description="Accounts Receivable", book_value=69_553.24, liquidation_recovery_pct=0.30, liquidation_value=20_865.97),
    _FakeAsset(asset_type="equipment", description="Plant & Equipment", book_value=15_000.0, liquidation_recovery_pct=0.25, liquidation_value=3_750.0),
]
_creditors = [
    _FakeCreditor(creditor_name="Supplier A", amount_claimed=200_000.0),
    _FakeCreditor(creditor_name="Supplier B", amount_claimed=150_000.0),
    _FakeCreditor(creditor_name="ATO", amount_claimed=100_000.0),
]
_narratives = [
    _FakeNarrative(section="background", content="Background content.", status="approved"),
    _FakeNarrative(section="distress_events", content="Distress events content.", status="draft"),
    _FakeNarrative(section="expert_advice", content="Expert advice content.", status="draft"),
    _FakeNarrative(section="plan_summary", content="Plan summary content.", status="reviewed"),
    _FakeNarrative(section="viability", content="Viability content.", status="approved"),
    _FakeNarrative(section="comparison_commentary", content="Comparison commentary.", status="draft"),
]


# ---------------------------------------------------------------------------
#  Cleanup helper
# ---------------------------------------------------------------------------

def _cleanup_generated_docs():
    """Remove any test-generated .docx files."""
    from services.document_generator import OUTPUT_DIR
    if OUTPUT_DIR.exists():
        for f in OUTPUT_DIR.glob("*Test_Co*.docx"):
            os.remove(str(f))


# ===========================================================================
#  POST /generate/comparison
# ===========================================================================


@pytest.mark.asyncio
async def test_generate_comparison_returns_200():
    """POST /generate/comparison with valid data returns 200 and DocumentResponse."""
    # DB queries: company, plan, creditors, assets
    mock_db = _build_mock_db([_company, _plan, _creditors, _assets])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison",
                json={},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["document_type"] == "Annexure G — Comparison"
        assert data["filename"].endswith(".docx")
        assert data["download_url"].startswith("/documents/")
        assert data["company_name"] == "Test Co Pty Ltd"
    finally:
        app.dependency_overrides.clear()
        _cleanup_generated_docs()


@pytest.mark.asyncio
async def test_generate_comparison_missing_plan_returns_400():
    """POST /generate/comparison without plan parameters returns 400."""
    mock_db = _build_mock_db([_company, None])  # company found, no plan
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison",
                json={},
            )
        assert resp.status_code == 400
        assert "plan parameters" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_generate_comparison_not_found_returns_404():
    """POST /generate/comparison for nonexistent engagement returns 404."""
    mock_db = _build_mock_db([None])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{uuid.uuid4()}/generate/comparison",
                json={},
            )
        assert resp.status_code == 404
    finally:
        app.dependency_overrides.clear()


# ===========================================================================
#  POST /generate/payment-schedule
# ===========================================================================


@pytest.mark.asyncio
async def test_generate_payment_schedule_returns_200():
    """POST /generate/payment-schedule with valid data returns 200."""
    # DB queries: company, plan
    mock_db = _build_mock_db([_company, _plan])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/payment-schedule",
                json={},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["document_type"] == "Payment Schedule"
        assert data["filename"].endswith(".docx")
        assert data["download_url"].startswith("/documents/")
    finally:
        app.dependency_overrides.clear()
        _cleanup_generated_docs()


@pytest.mark.asyncio
async def test_generate_payment_schedule_with_dates():
    """POST /generate/payment-schedule with optional dates works."""
    mock_db = _build_mock_db([_company, _plan])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/payment-schedule",
                json={
                    "appointment_date": "2026-01-15",
                    "document_date": "2026-02-19",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["document_type"] == "Payment Schedule"
    finally:
        app.dependency_overrides.clear()
        _cleanup_generated_docs()


@pytest.mark.asyncio
async def test_generate_payment_schedule_missing_plan_returns_400():
    """POST /generate/payment-schedule without plan returns 400."""
    mock_db = _build_mock_db([_company, None])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/payment-schedule",
                json={},
            )
        assert resp.status_code == 400
        assert "plan parameters" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


# ===========================================================================
#  POST /generate/company-statement
# ===========================================================================


@pytest.mark.asyncio
async def test_generate_company_statement_returns_200():
    """POST /generate/company-statement with narratives returns 200."""
    # DB queries: company, narratives
    mock_db = _build_mock_db([_company, _narratives])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/company-statement",
                json={"practitioner_name": "Jane Smith"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["document_type"] == "Company Offer Statement"
        assert data["filename"].endswith(".docx")
        assert data["practitioner_name"] == "Jane Smith"
    finally:
        app.dependency_overrides.clear()
        _cleanup_generated_docs()


@pytest.mark.asyncio
async def test_generate_company_statement_missing_narratives_returns_400():
    """POST /generate/company-statement with no narratives returns 400."""
    mock_db = _build_mock_db([_company, []])  # company found, empty narratives
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/company-statement",
                json={},
            )
        assert resp.status_code == 400
        assert "narrative" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


# ===========================================================================
#  POST /generate/full-pack
# ===========================================================================


@pytest.mark.asyncio
async def test_generate_full_pack_returns_3_documents():
    """POST /generate/full-pack returns all three documents."""
    # DB queries for full-pack:
    # 1. company (from _load_company_or_404)
    # 2. plan (from _load_comparison_data)
    # 3. creditors (from _load_comparison_data)
    # 4. assets (from _load_comparison_data)
    # 5. plan (from _load_schedule_data)
    # 6. narratives (from full-pack)
    mock_db = _build_mock_db([
        _company,       # company lookup
        _plan,          # comparison: plan
        _creditors,     # comparison: creditors
        _assets,        # comparison: assets
        _plan,          # schedule: plan
        _narratives,    # statement: narratives
    ])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/full-pack",
                json={"practitioner_name": "Test Practitioner"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 3
        assert len(data["documents"]) == 3

        doc_types = {d["document_type"] for d in data["documents"]}
        assert "Annexure G — Comparison" in doc_types
        assert "Payment Schedule" in doc_types
        assert "Company Offer Statement" in doc_types

        for doc in data["documents"]:
            assert doc["filename"].endswith(".docx")
            assert doc["download_url"].startswith("/documents/")
    finally:
        app.dependency_overrides.clear()
        _cleanup_generated_docs()


@pytest.mark.asyncio
async def test_generate_full_pack_missing_data_returns_400():
    """POST /generate/full-pack fails with 400 if plan is missing."""
    mock_db = _build_mock_db([_company, None])  # company found, no plan
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/full-pack",
                json={},
            )
        assert resp.status_code == 400
    finally:
        app.dependency_overrides.clear()


# ===========================================================================
#  GET /documents
# ===========================================================================


@pytest.mark.asyncio
async def test_list_documents_empty():
    """GET /documents for engagement with no docs returns empty list."""
    mock_db = _build_mock_db([_company])
    _override_db(mock_db)
    try:
        _cleanup_generated_docs()  # ensure clean state
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/documents",
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["engagement_id"] == str(_TEST_COMPANY_ID)
        assert data["company_name"] == "Test Co Pty Ltd"
        assert isinstance(data["documents"], list)
        assert data["count"] >= 0
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_list_documents_after_generation():
    """Generate a doc then verify GET /documents lists it."""
    # First, generate a payment schedule (simplest — only needs company + plan)
    mock_db_gen = _build_mock_db([_company, _plan])
    _override_db(mock_db_gen)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            gen_resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/payment-schedule",
                json={},
            )
        assert gen_resp.status_code == 200
    finally:
        app.dependency_overrides.clear()

    # Now list documents
    mock_db_list = _build_mock_db([_company])
    _override_db(mock_db_list)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            list_resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/documents",
            )
        assert list_resp.status_code == 200
        data = list_resp.json()
        assert data["count"] >= 1
        doc_names = [d["filename"] for d in data["documents"]]
        assert any("PaymentSchedule" in n for n in doc_names)
    finally:
        app.dependency_overrides.clear()
        _cleanup_generated_docs()


@pytest.mark.asyncio
async def test_list_documents_not_found_returns_404():
    """GET /documents for nonexistent engagement returns 404."""
    mock_db = _build_mock_db([None])
    _override_db(mock_db)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{uuid.uuid4()}/documents",
            )
        assert resp.status_code == 404
    finally:
        app.dependency_overrides.clear()


# ===========================================================================
#  Regression: existing endpoints unaffected
# ===========================================================================


@pytest.mark.asyncio
async def test_existing_health_endpoint_unaffected():
    """GET /health still returns 200."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
