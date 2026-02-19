"""Tests for 3.2 document download + generation endpoints."""

from __future__ import annotations

import io
import uuid
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from docx import Document as DocxDocument
from httpx import ASGITransport, AsyncClient

from main import app


# ---------------------------------------------------------------------------
#  Fake DB models (following test_api_endpoints.py patterns)
# ---------------------------------------------------------------------------


class _FakeCompany:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.legal_name = kwargs.get("legal_name", "Test Co")
        self.acn = kwargs.get("acn")
        self.abn = kwargs.get("abn")
        self.total_creditors = kwargs.get("total_creditors", 0)
        self.created_at = "2026-01-01T00:00:00"


class _FakeCreditor:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.company_id = kwargs.get("company_id")
        self.creditor_name = kwargs.get("creditor_name", "Creditor A")
        self.amount_claimed = kwargs.get("amount_claimed", 100000)
        self.category = kwargs.get("category", "trade")
        self.status = kwargs.get("status", "active")
        self.is_related_party = kwargs.get("is_related_party", False)
        self.is_secured = kwargs.get("is_secured", False)
        self.can_vote = kwargs.get("can_vote", True)
        self.notes = kwargs.get("notes")


class _FakeAsset:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.company_id = kwargs.get("company_id")
        self.asset_type = kwargs.get("asset_type", "cash")
        self.description = kwargs.get("description", "Cash at Bank")
        self.book_value = kwargs.get("book_value", 100000)
        self.liquidation_recovery_pct = kwargs.get("liquidation_recovery_pct", 0.20)
        self.liquidation_value = kwargs.get("liquidation_value", 20000)


class _FakePlan:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.company_id = kwargs.get("company_id")
        self.total_contribution = kwargs.get("total_contribution", 516000.0)
        self.practitioner_fee_pct = kwargs.get("practitioner_fee_pct", 10.0)
        self.num_initial_payments = kwargs.get("num_initial_payments", 2)
        self.initial_payment_amount = kwargs.get("initial_payment_amount", 32500.0)
        self.num_ongoing_payments = kwargs.get("num_ongoing_payments", 22)
        self.ongoing_payment_amount = kwargs.get("ongoing_payment_amount", 20500.0)
        self.est_liquidator_fees = kwargs.get("est_liquidator_fees", 50000.0)
        self.est_legal_fees = kwargs.get("est_legal_fees", 10000.0)
        self.est_disbursements = kwargs.get("est_disbursements", 5000.0)


class _FakeNarrative:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.engagement_id = kwargs.get("engagement_id")
        self.section = kwargs.get("section", "background")
        self.content = kwargs.get("content", "Test content for this section.")
        self.status = kwargs.get("status", "draft")
        self.metadata_ = kwargs.get("metadata_")
        self.entity_map = kwargs.get("entity_map")


class _FakeDocumentOutput:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.engagement_id = kwargs.get("engagement_id")
        self.document_type = kwargs.get("document_type", "comparison")
        self.version = kwargs.get("version", 1)
        self.filename = kwargs.get("filename", "test.docx")
        self.generated_at = kwargs.get("generated_at", "2026-02-19T10:00:00+00:00")
        self.metadata_ = kwargs.get("metadata_")


class _FakeScalarResult:
    """Mimics SQLAlchemy scalar result for .scalar_one_or_none()."""

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


# ---------------------------------------------------------------------------
#  Test data
# ---------------------------------------------------------------------------

_TEST_COMPANY_ID = uuid.uuid4()

_TEST_COMPANY = _FakeCompany(
    id=_TEST_COMPANY_ID,
    legal_name="Point Blank Medical Pty Ltd",
    acn="123456789",
    total_creditors=985777.37,
)

_TEST_CREDITORS = [
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="Australian Taxation Office - ITA",
        amount_claimed=573230.31,
        category="ato_ita",
    ),
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="Prospa Advance",
        amount_claimed=143874.02,
        category="finance",
    ),
]

_TEST_ASSETS = [
    _FakeAsset(
        company_id=_TEST_COMPANY_ID,
        asset_type="cash",
        description="Cash at Bank",
        book_value=59689.27,
        liquidation_recovery_pct=0.20,
        liquidation_value=59689.27 * 0.20,
    ),
    _FakeAsset(
        company_id=_TEST_COMPANY_ID,
        asset_type="receivables",
        description="Accounts Receivable",
        book_value=69553.24,
        liquidation_recovery_pct=0.30,
        liquidation_value=69553.24 * 0.30,
    ),
    _FakeAsset(
        company_id=_TEST_COMPANY_ID,
        asset_type="equipment",
        description="Plant & Equipment",
        book_value=15000.00,
        liquidation_recovery_pct=0.25,
        liquidation_value=15000.00 * 0.25,
    ),
]

_TEST_PLAN = _FakePlan(company_id=_TEST_COMPANY_ID)

_TEST_NARRATIVES = [
    _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="background",
        content="Point Blank Medical Pty Ltd was incorporated in 2018.",
        status="approved",
    ),
    _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="distress_events",
        content="In mid-2025, the company lost a major supply contract.",
        status="draft",
    ),
    _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="expert_advice",
        content="The directors sought professional advice in January 2026.",
        status="draft",
    ),
    _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="plan_summary",
        content="The proposed plan involves a total contribution of $516,000.",
        status="reviewed",
    ),
    _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="viability",
        content="The company has implemented cost reductions.",
        status="approved",
    ),
    _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="comparison_commentary",
        content="Under the SBR plan, creditors receive 47.1 cents in the dollar.",
        status="approved",
    ),
]


# ---------------------------------------------------------------------------
#  Mock DB builder
# ---------------------------------------------------------------------------


def _build_mock_db(results_in_order: list):
    """
    Build an AsyncMock db session returning controlled query results.
    Supports tracking added records for DocumentOutputDB verification.
    """
    mock_db = AsyncMock()
    _call_count = {"n": 0}
    mock_db._added_records = []

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

    def _add(x):
        mock_db._added_records.append(x)

    mock_db.add = _add
    return mock_db


def _get_db_override(mock_db):
    """Create a get_db override that yields the mock_db."""
    from db.database import get_db as _original_get_db

    async def _override():
        yield mock_db

    return _original_get_db, _override


# ---------------------------------------------------------------------------
#  1. test_generate_comparison_returns_docx
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_comparison_returns_docx():
    """POST /generate/comparison returns 200 with correct content-type."""
    # Queries: company, plan, creditors, assets, max(version), (flush)
    mock_db = _build_mock_db([
        _TEST_COMPANY,    # select CompanyDB
        _TEST_PLAN,       # select PlanParametersDB
        _TEST_CREDITORS,  # select CreditorDB
        _TEST_ASSETS,     # select AssetDB
        0,                # max(version) for document_output tracking
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp.status_code == 200
        assert "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in resp.headers.get("content-type", "")
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  2. test_generate_comparison_valid_docx
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_comparison_valid_docx():
    """Response body is a valid .docx file parseable by python-docx."""
    mock_db = _build_mock_db([
        _TEST_COMPANY, _TEST_PLAN, _TEST_CREDITORS, _TEST_ASSETS, 0,
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp.status_code == 200
        doc = DocxDocument(io.BytesIO(resp.content))
        assert len(doc.tables) > 0  # comparison doc has tables
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  3. test_generate_comparison_missing_data
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_comparison_missing_data():
    """Returns 400 if no comparison data (no plan parameters)."""
    mock_db = _build_mock_db([
        _TEST_COMPANY,  # company exists
        None,           # no plan parameters
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp.status_code == 400
        assert "plan parameters" in resp.json()["detail"].lower() or "Set plan parameters first" in resp.json()["detail"]
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  4. test_generate_payment_schedule_returns_docx
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_payment_schedule_returns_docx():
    """POST /generate/payment-schedule returns 200 with correct content-type."""
    # Queries: company, plan, max(version)
    mock_db = _build_mock_db([
        _TEST_COMPANY,  # select CompanyDB
        _TEST_PLAN,     # select PlanParametersDB
        0,              # max(version)
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/payment-schedule"
            )
        assert resp.status_code == 200
        assert "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in resp.headers.get("content-type", "")
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  5. test_generate_payment_schedule_missing_params
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_payment_schedule_missing_params():
    """Returns 400 if no plan parameters."""
    mock_db = _build_mock_db([
        _TEST_COMPANY,  # company exists
        None,           # no plan params
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/payment-schedule"
            )
        assert resp.status_code == 400
        assert "plan parameters" in resp.json()["detail"].lower() or "Set plan parameters first" in resp.json()["detail"]
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  6. test_generate_company_statement_returns_docx
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_company_statement_returns_docx():
    """POST /generate/company-statement returns 200 with correct content-type."""
    # Queries: company, narratives, max(version)
    mock_db = _build_mock_db([
        _TEST_COMPANY,      # select CompanyDB
        _TEST_NARRATIVES,   # select NarrativeDB
        0,                  # max(version)
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/company-statement"
            )
        assert resp.status_code == 200
        assert "application/vnd.openxmlformats-officedocument.wordprocessingml.document" in resp.headers.get("content-type", "")
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  7. test_generate_company_statement_draft_header
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_company_statement_draft_header():
    """Response includes X-Draft-Sections header if unapproved sections exist."""
    mock_db = _build_mock_db([
        _TEST_COMPANY, _TEST_NARRATIVES, 0,
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/company-statement"
            )
        assert resp.status_code == 200
        draft_header = resp.headers.get("x-draft-sections", "")
        assert draft_header  # should be non-empty since some sections are draft/reviewed
        # distress_events, expert_advice, plan_summary are not approved
        assert "distress_events" in draft_header
        assert "expert_advice" in draft_header
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  8. test_generate_all_returns_zip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_all_returns_zip():
    """POST /generate/all returns ZIP with correct content-type."""
    # Queries: company, plan, creditors, assets, narratives,
    #          + 3x max(version) for tracking
    mock_db = _build_mock_db([
        _TEST_COMPANY,      # select CompanyDB
        _TEST_PLAN,         # select PlanParametersDB
        _TEST_CREDITORS,    # select CreditorDB
        _TEST_ASSETS,       # select AssetDB
        _TEST_NARRATIVES,   # select NarrativeDB
        0,                  # max(version) for comparison
        0,                  # max(version) for payment_schedule
        0,                  # max(version) for company_statement
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/all"
            )
        assert resp.status_code == 200
        assert "application/zip" in resp.headers.get("content-type", "")
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  9. test_generate_all_contains_three_files
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_all_contains_three_files():
    """ZIP contains 3 .docx files."""
    mock_db = _build_mock_db([
        _TEST_COMPANY, _TEST_PLAN, _TEST_CREDITORS, _TEST_ASSETS,
        _TEST_NARRATIVES, 0, 0, 0,
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/all"
            )
        assert resp.status_code == 200
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        names = zf.namelist()
        assert len(names) == 3
        for name in names:
            assert name.endswith(".docx")
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  10. test_generate_invalid_engagement
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_invalid_engagement():
    """Returns 404 for nonexistent engagement."""
    fake_id = uuid.uuid4()
    mock_db = _build_mock_db([None])  # company not found

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{fake_id}/generate/comparison"
            )
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  11. test_document_output_tracked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_document_output_tracked():
    """After generation, DocumentOutputDB has a record (mock.add called)."""
    mock_db = _build_mock_db([
        _TEST_COMPANY, _TEST_PLAN, _TEST_CREDITORS, _TEST_ASSETS, 0,
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp.status_code == 200
        # Verify that a DocumentOutputDB record was added via db.add()
        from db.models import DocumentOutputDB
        tracked_records = [
            r for r in mock_db._added_records
            if isinstance(r, DocumentOutputDB)
        ]
        assert len(tracked_records) >= 1
        assert tracked_records[0].document_type == "comparison"
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  12. test_document_version_increments
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_document_version_increments():
    """Generate twice, version goes from 1 to 2."""
    # First generation: max version = 0 → new version = 1
    mock_db_1 = _build_mock_db([
        _TEST_COMPANY, _TEST_PLAN, _TEST_CREDITORS, _TEST_ASSETS, 0,
    ])

    original_get_db, override1 = _get_db_override(mock_db_1)
    app.dependency_overrides[original_get_db] = override1
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp1 = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp1.status_code == 200
        from db.models import DocumentOutputDB
        records_1 = [r for r in mock_db_1._added_records if isinstance(r, DocumentOutputDB)]
        assert records_1[0].version == 1
    finally:
        app.dependency_overrides.clear()

    # Second generation: max version = 1 → new version = 2
    mock_db_2 = _build_mock_db([
        _TEST_COMPANY, _TEST_PLAN, _TEST_CREDITORS, _TEST_ASSETS, 1,
    ])

    _, override2 = _get_db_override(mock_db_2)
    app.dependency_overrides[original_get_db] = override2
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp2 = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp2.status_code == 200
        records_2 = [r for r in mock_db_2._added_records if isinstance(r, DocumentOutputDB)]
        assert records_2[0].version == 2
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  13. test_list_documents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_documents():
    """GET /documents returns history."""
    fake_docs = [
        _FakeDocumentOutput(
            engagement_id=_TEST_COMPANY_ID,
            document_type="comparison",
            version=1,
            filename="Point_Blank_Medical_Annexure_G_Comparison_19022026.docx",
        ),
        _FakeDocumentOutput(
            engagement_id=_TEST_COMPANY_ID,
            document_type="payment_schedule",
            version=1,
            filename="Point_Blank_Medical_Payment_Schedule_19022026.docx",
        ),
    ]

    mock_db = _build_mock_db([
        _TEST_COMPANY,  # validate engagement
        fake_docs,      # select DocumentOutputDB
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/documents"
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["engagement_id"] == str(_TEST_COMPANY_ID)
        assert len(data["documents"]) == 2
        assert data["documents"][0]["document_type"] == "comparison"
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  14. test_list_documents_empty
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_documents_empty():
    """GET /documents for new engagement returns empty list."""
    mock_db = _build_mock_db([
        _TEST_COMPANY,  # validate engagement
        [],             # no document outputs
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/documents"
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["documents"] == []
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  15. test_filename_format
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filename_format():
    """Filename matches expected pattern: no spaces, AU date format DDMMYYYY."""
    mock_db = _build_mock_db([
        _TEST_COMPANY, _TEST_PLAN, _TEST_CREDITORS, _TEST_ASSETS, 0,
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp.status_code == 200
        content_disp = resp.headers.get("content-disposition", "")
        # Filename should contain no spaces
        assert " " not in content_disp or "filename=" in content_disp
        # Extract filename from content-disposition
        import re
        match = re.search(r'filename="?([^";\n]+)"?', content_disp)
        if match:
            filename = match.group(1)
            assert " " not in filename
            assert filename.endswith(".docx")
            # Check AU date format DDMMYYYY (8 digits before .docx)
            assert re.search(r"\d{8}\.docx$", filename)
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  16. test_filename_uses_company_name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filename_uses_company_name():
    """Filename contains company name (sanitized)."""
    mock_db = _build_mock_db([
        _TEST_COMPANY, _TEST_PLAN, _TEST_CREDITORS, _TEST_ASSETS, 0,
    ])

    original_get_db, override = _get_db_override(mock_db)
    app.dependency_overrides[original_get_db] = override
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/generate/comparison"
            )
        assert resp.status_code == 200
        content_disp = resp.headers.get("content-disposition", "")
        # Should contain "Point_Blank_Medical" (sanitized version of company name)
        assert "Point_Blank_Medical" in content_disp
    finally:
        app.dependency_overrides.clear()
