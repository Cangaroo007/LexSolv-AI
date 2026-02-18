"""Tests for 2.3 Narrative and Glossary API endpoints.

All tests mock the Claude API — no real API calls.
Uses the same test client and DB mock patterns from test_api_endpoints.py.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from main import app
from services.claude_client import GenerateResult


# ---------------------------------------------------------------------------
#  Shared fixtures and helpers
# ---------------------------------------------------------------------------

_TEST_COMPANY_ID = uuid.uuid4()


class _FakeCompany:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.legal_name = kwargs.get("legal_name", "Test Co")
        self.acn = kwargs.get("acn")
        self.abn = kwargs.get("abn")
        self.total_creditors = kwargs.get("total_creditors", 0)
        self.custom_glossary = kwargs.get("custom_glossary", None)
        self.created_at = "2026-01-01T00:00:00"


class _FakeNarrative:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.engagement_id = kwargs.get("engagement_id", _TEST_COMPANY_ID)
        self.section = kwargs.get("section", "background")
        self.content = kwargs.get("content", "Generated background content.")
        self.status = kwargs.get("status", "draft")
        self.metadata_ = kwargs.get("metadata_", {
            "section": "background",
            "requires_input_flags": [],
            "unknown_terms_flagged": [],
        })
        self.entity_map = kwargs.get("entity_map", {})


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


def _mock_generate_result(section: str = "background") -> GenerateResult:
    """Return a mock Claude response with flags."""
    return GenerateResult(
        text=(
            f"The company was incorporated on [REQUIRES INPUT: date of incorporation] "
            f"and has traded in the {section} sector. "
            f"The company operates from its registered office."
        ),
        input_tokens=1000,
        output_tokens=500,
        model="claude-sonnet-4-20250514",
    )


# ---------------------------------------------------------------------------
# Narrative generation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("services.claude_client.ClaudeClient.generate")
async def test_generate_narrative_returns_6_sections(mock_generate: AsyncMock):
    """POST with director notes returns 6 sections."""
    mock_generate.return_value = _mock_generate_result()

    _company = _FakeCompany(id=_TEST_COMPANY_ID, legal_name="Test Co Pty Ltd")
    # generate_narrative queries: company, plan, (assets, creditors if plan present)
    mock_db = _build_mock_db([
        _company,   # 1st: select CompanyDB
        None,       # 2nd: select PlanParametersDB (none)
    ])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative",
                json={
                    "director_notes": "The company was founded in 2018 and traded in medical supplies.",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["engagement_id"] == str(_TEST_COMPANY_ID)
        assert len(data["sections"]) == 6
        assert "generated_at" in data

        # Verify all 6 section names are present
        section_names = {s["section"] for s in data["sections"]}
        expected = {"background", "distress_events", "expert_advice", "plan_summary", "viability", "comparison_commentary"}
        assert section_names == expected

        # Each section should have status="draft"
        for s in data["sections"]:
            assert s["status"] == "draft"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_generate_narrative_missing_notes():
    """POST without director_notes returns 400."""
    from db.database import get_db as _original_get_db

    mock_db = _build_mock_db([])

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative",
                json={"director_notes": ""},
            )
        assert resp.status_code == 400
        assert "director_notes" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_generate_narrative_invalid_engagement():
    """POST to nonexistent engagement returns 404."""
    fake_id = uuid.uuid4()

    from db.database import get_db as _original_get_db

    mock_db = _build_mock_db([None])  # Company not found

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{fake_id}/narrative",
                json={"director_notes": "Some notes."},
            )
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
@patch("services.claude_client.ClaudeClient.generate")
async def test_generate_single_section(mock_generate: AsyncMock):
    """POST to /narrative/background returns single section."""
    mock_generate.return_value = _mock_generate_result("background")

    _company = _FakeCompany(id=_TEST_COMPANY_ID, legal_name="Test Co Pty Ltd")
    mock_db = _build_mock_db([
        _company,   # 1st: select CompanyDB
        None,       # 2nd: select PlanParametersDB
    ])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative/background",
                json={"director_notes": "The company was founded in 2018."},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["section"] == "background"
        assert data["status"] == "draft"
        assert "content" in data
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_generate_invalid_section_name():
    """POST to /narrative/invalid_name returns 400."""
    _company = _FakeCompany(id=_TEST_COMPANY_ID, legal_name="Test Co Pty Ltd")
    mock_db = _build_mock_db([_company])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative/invalid_name",
                json={"director_notes": "Some notes."},
            )
        assert resp.status_code == 400
        assert "invalid section name" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Section management tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_approve_section():
    """PATCH with status='approved' updates and persists."""
    narrative = _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="background",
        status="draft",
    )
    mock_db = _build_mock_db([narrative])  # select NarrativeDB

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.patch(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative/background",
                json={"status": "approved"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "approved"
        assert data["section"] == "background"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_edit_section_content():
    """PATCH with new content updates the section."""
    narrative = _FakeNarrative(
        engagement_id=_TEST_COMPANY_ID,
        section="background",
        content="Original content.",
    )
    mock_db = _build_mock_db([narrative])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.patch(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative/background",
                json={"content": "Edited content from practitioner."},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["content"] == "Edited content from practitioner."
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_all_sections():
    """GET /narrative returns all sections with status."""
    narratives = [
        _FakeNarrative(section="background", status="draft"),
        _FakeNarrative(section="distress_events", status="reviewed"),
        _FakeNarrative(section="expert_advice", status="approved"),
    ]
    _company = _FakeCompany(id=_TEST_COMPANY_ID)
    mock_db = _build_mock_db([
        _company,     # 1st: select CompanyDB
        narratives,   # 2nd: select NarrativeDB list
    ])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative",
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["engagement_id"] == str(_TEST_COMPANY_ID)
        assert len(data["sections"]) == 3
        assert data["all_approved"] is False
        assert "requires_input_count" in data
        assert "unknown_terms_count" in data
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_single_section():
    """GET /narrative/background returns just that section."""
    narrative = _FakeNarrative(
        section="background",
        content="Generated background.",
        status="draft",
    )
    mock_db = _build_mock_db([narrative])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative/background",
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["section"] == "background"
        assert data["content"] == "Generated background."
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_all_approved_flag():
    """Approve all 6 sections, verify all_approved=true."""
    narratives = [
        _FakeNarrative(section="background", status="approved"),
        _FakeNarrative(section="distress_events", status="approved"),
        _FakeNarrative(section="expert_advice", status="approved"),
        _FakeNarrative(section="plan_summary", status="approved"),
        _FakeNarrative(section="viability", status="approved"),
        _FakeNarrative(section="comparison_commentary", status="approved"),
    ]
    _company = _FakeCompany(id=_TEST_COMPANY_ID)
    mock_db = _build_mock_db([
        _company,     # 1st: select CompanyDB
        narratives,   # 2nd: select NarrativeDB list
    ])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/narrative",
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["all_approved"] is True
        assert len(data["sections"]) == 6
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Glossary tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_insolvency_glossary():
    """GET /glossary/insolvency returns terms."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/glossary/insolvency")
    assert resp.status_code == 200
    data = resp.json()
    assert data["layer"] == "insolvency"
    assert data["term_count"] == 17
    assert "SBR" in data["terms"]
    assert "DIRRI" in data["terms"]


@pytest.mark.asyncio
async def test_get_medical_glossary():
    """GET /glossary/medical returns terms."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/glossary/medical")
    assert resp.status_code == 200
    data = resp.json()
    assert data["layer"] == "medical"
    assert data["term_count"] == 10
    assert "allograft" in data["terms"]
    assert "TGA" in data["terms"]


@pytest.mark.asyncio
async def test_get_unknown_glossary():
    """GET /glossary/construction returns 404 with available list."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/api/glossary/construction")
    assert resp.status_code == 404
    data = resp.json()["detail"]
    assert "available_industries" in data
    assert "medical" in data["available_industries"]


@pytest.mark.asyncio
async def test_add_custom_terms():
    """POST custom terms stores and returns them."""
    _company = _FakeCompany(id=_TEST_COMPANY_ID, custom_glossary=None)
    mock_db = _build_mock_db([_company])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/glossary/terms",
                json={
                    "terms": {
                        "BTC Health": "Former exclusive supplier of allograft tissue products",
                        "BlueShak": "Related-party entity controlled by the director",
                    }
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["engagement_id"] == str(_TEST_COMPANY_ID)
        assert "BTC Health" in data["custom_terms"]
        assert "BlueShak" in data["custom_terms"]
        assert data["total_terms"] > 0
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_custom_terms_merge():
    """Verify custom terms appear in merged glossary."""
    existing_custom = {"ExistingTerm": "An existing custom term"}
    _company = _FakeCompany(id=_TEST_COMPANY_ID, custom_glossary=existing_custom)
    mock_db = _build_mock_db([_company])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/glossary/terms",
                json={
                    "terms": {
                        "NewTerm": "A new term added by the client",
                    }
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        # Both existing and new custom terms present
        assert "ExistingTerm" in data["custom_terms"]
        assert "NewTerm" in data["custom_terms"]
        # Merged glossary includes Layer 1 + custom
        assert "SBR" in data["merged_glossary"]
        assert "NewTerm" in data["merged_glossary"]
        assert "ExistingTerm" in data["merged_glossary"]
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Regression: existing endpoints still work
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_existing_health_endpoint_still_works():
    """GET /health → 200 (Rule 3 regression check)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
