"""Tests for 1.4 API endpoints — file upload, engagement CRUD, comparison + payment schedule."""

from __future__ import annotations

import io
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from main import app

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------


def _read_fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


# ---------------------------------------------------------------------------
#  A: File Upload Endpoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_aged_payables_returns_creditors():
    """POST PBM CSV fixture → 200, response has 'creditors' list with 6 items."""
    content = _read_fixture("pbm_aged_payables.csv")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/upload/aged-payables",
            files={"file": ("pbm_aged_payables.csv", io.BytesIO(content), "text/csv")},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "creditors" in data
    assert len(data["creditors"]) == 6
    assert data["count"] == 6
    assert "parse_method" in data


@pytest.mark.asyncio
async def test_upload_balance_sheet_returns_assets():
    """POST PBM balance sheet → 200, response has 'assets' list."""
    content = _read_fixture("pbm_balance_sheet.csv")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/upload/balance-sheet",
            files={"file": ("pbm_balance_sheet.csv", io.BytesIO(content), "text/csv")},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "assets" in data
    assert len(data["assets"]) > 0
    assert data["parse_method"] == "keyword_match"


@pytest.mark.asyncio
async def test_upload_wrong_format_returns_400():
    """POST a .txt file → 400 with helpful message."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/upload/aged-payables",
            files={"file": ("test.txt", io.BytesIO(b"some text"), "text/plain")},
        )
    assert resp.status_code == 400
    assert "Supported formats" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_upload_empty_file_returns_400():
    """POST empty file → 400."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            "/api/upload/aged-payables",
            files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
        )
    assert resp.status_code == 400
    assert "empty" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
#  B/C: Engagement + Comparison + Payment Schedule (with DB mocking)
# ---------------------------------------------------------------------------


# Fake DB models for testing
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


# PBM test data matching the fixture values
_TEST_COMPANY_ID = uuid.uuid4()

_PBM_CREDITORS = [
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="Australian Taxation Office - ITA",
        amount_claimed=573230.31,
        category="ato_ita",
    ),
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="Australian Taxation Office - ICA",
        amount_claimed=268294.01,
        category="ato_ica",
    ),
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="iCare NSW",
        amount_claimed=825.23,
        category="workers_comp",
    ),
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="Prospa Advance",
        amount_claimed=143874.02,
        category="finance",
    ),
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="BlueShak",
        amount_claimed=142105.81,
        category="trade",
    ),
    _FakeCreditor(
        company_id=_TEST_COMPANY_ID,
        creditor_name="BTC Health Australia",
        amount_claimed=67447.99,
        category="trade",
    ),
]

# Total claims from the creditors above: 1,195,777.37
# Using balance sheet total liabilities as creditor total for comparison: 985,777.37
_PBM_ASSETS = [
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
        asset_type="inventory",
        description="Inventory",
        book_value=51826.62,
        liquidation_recovery_pct=0.25,
        liquidation_value=51826.62 * 0.25,
    ),
    _FakeAsset(
        company_id=_TEST_COMPANY_ID,
        asset_type="loans_related",
        description="Loans to Related Entities",
        book_value=34964.83,
        liquidation_recovery_pct=0.30,
        liquidation_value=34964.83 * 0.30,
    ),
    _FakeAsset(
        company_id=_TEST_COMPANY_ID,
        asset_type="loans_shareholder",
        description="Shareholder Loans",
        book_value=2010000.00,
        liquidation_recovery_pct=0.00,
        liquidation_value=0.0,
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

_PBM_PLAN = _FakePlan(company_id=_TEST_COMPANY_ID)


def _build_mock_db(results_in_order: list):
    """
    Build an AsyncMock db session returning controlled query results.
    results_in_order: list of objects to return for each successive db.execute() call.
    Each entry can be a single object (for .scalar_one_or_none()) or a list (for .scalars().all()).
    """
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


@pytest.mark.asyncio
async def test_comparison_endpoint_returns_result():
    """Create engagement with test data, POST compare → 200 with dividend figures."""
    # run_comparison queries in order: company, plan, creditors, assets
    _test_company = _FakeCompany(
        id=_TEST_COMPANY_ID,
        legal_name="Point Blank Medical Pty Ltd",
        total_creditors=985777.37,
    )
    mock_db = _build_mock_db([
        _test_company,    # 1st query: select CompanyDB -> .scalar_one_or_none()
        _PBM_PLAN,        # 2nd query: select PlanParametersDB -> .scalar_one_or_none()
        _PBM_CREDITORS,   # 3rd query: select CreditorDB -> .scalars().all()
        _PBM_ASSETS,      # 4th query: select AssetDB -> .scalars().all()
    ])

    from db.database import get_db as _original_get_db

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_TEST_COMPANY_ID}/compare"
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "sbr_dividend_cents" in data
        assert "liquidation_dividend_cents" in data
        assert "sbr_available" in data
        assert "liquidation_available" in data
        assert data["sbr_dividend_cents"] > 0
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_payment_schedule_endpoint():
    """Create engagement with plan params → 200 with schedule entries."""
    from db.database import get_db as _original_get_db

    # get_payment_schedule queries: plan
    mock_db = _build_mock_db([_PBM_PLAN])

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[_original_get_db] = _override_get_db
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_TEST_COMPANY_ID}/payment-schedule"
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data
        assert len(data["entries"]) == 24  # 2 initial + 22 ongoing
        assert data["total_contribution"] == 516000.0
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
#  Existing endpoint regression tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_existing_health_endpoint_still_works():
    """GET /health → 200."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_existing_forensic_endpoint_still_works():
    """Verify at least one existing forensic route responds (Rule 3)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # POST to solvency-score with valid body
        resp = await client.post(
            "/api/forensic/solvency-score",
            json={
                "current_assets": "100000.00",
                "current_liabilities": "200000.00",
            },
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "score" in data
    assert "recommendation" in data
