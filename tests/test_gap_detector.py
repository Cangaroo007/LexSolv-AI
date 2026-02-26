"""Tests for the Gap Detection Engine (Prompt 5.3 — Task D).

Tests gap detection, gap filling, director questionnaire, practitioner
checklist, and the comparison engine gate.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from main import app
from services.gap_detector import GapDetector
from services.parser_merger import MergedParseResult


# ---------------------------------------------------------------------------
# Helpers — MergedParseResult builders
# ---------------------------------------------------------------------------


def _make_merged(
    document_type: str,
    fields: dict,
    confidence: dict[str, float] | None = None,
) -> MergedParseResult:
    """Build a minimal MergedParseResult for testing."""
    if confidence is None:
        confidence = {k: 0.95 for k in fields}
    return MergedParseResult(
        document_type=document_type,
        fields=fields,
        confidence=confidence,
        source={k: "test" for k in fields},
        conflicts=[],
        parse_summary="test fixture",
    )


def _complete_uploaded_documents() -> dict[str, MergedParseResult | None]:
    """All documents uploaded with complete data — 0 blocking gaps expected."""
    return {
        "aged_payables": _make_merged("aged_payables", {
            "creditors": [
                {"creditor_name": "ATO", "amount_claimed": 573230.31, "category": "ato_ita", "is_related_party": False},
                {"creditor_name": "Trade Creditor", "amount_claimed": 50000.0, "category": "trade", "is_related_party": False},
            ],
            "total_claims": 623230.31,
            "creditor[*].category": ["ato_ita", "trade"],
            "creditor[*].is_related_party": [False, False],
        }),
        "balance_sheet": _make_merged("balance_sheet", {
            "total_liabilities": 985777.37,
            "assets": [
                {"asset_type": "cash", "book_value": 59689.27, "recovery_pct": 0.20},
                {"asset_type": "receivables", "book_value": 69553.24, "recovery_pct": 0.30},
            ],
            "asset[*].recovery_pct": [0.20, 0.30],
        }),
        "bank_statement": _make_merged("bank_statement", {
            "closing_balance": 59689.27,
            "period_end_date": "2025-12-31",
        }),
        "pnl": None,
    }


def _complete_plan_parameters() -> dict:
    return {
        "total_contribution": 516000.0,
        "practitioner_fee_pct": 10.0,
        "num_initial_payments": 2,
        "initial_payment_amount": 32500.0,
    }


# ---------------------------------------------------------------------------
# Fake DB models (matching test_api_endpoints.py pattern)
# ---------------------------------------------------------------------------


class _FakeCompany:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.legal_name = kwargs.get("legal_name", "Test Co")
        self.acn = kwargs.get("acn")
        self.abn = kwargs.get("abn")
        self.total_creditors = kwargs.get("total_creditors", 0)
        self.total_liabilities = kwargs.get("total_liabilities", 985777.37)
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


class _FakeGapFill:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.engagement_id = kwargs.get("engagement_id")
        self.field_name = kwargs.get("field_name", "closing_balance")
        self.document_type = kwargs.get("document_type", "bank_statement")
        self.filled_value = kwargs.get("filled_value", 59689.27)
        self.filled_by = kwargs.get("filled_by", "practitioner")
        self.filled_at = kwargs.get("filled_at", datetime.now(timezone.utc))
        self.previous_value = kwargs.get("previous_value")
        self.confidence = kwargs.get("confidence", 1.0)
        self.notes = kwargs.get("notes")


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
# Unit Tests — GapDetector service (no DB)
# ---------------------------------------------------------------------------


_TEST_COMPANY_ID = str(uuid.uuid4())
detector = GapDetector()


class TestGapDetectorService:
    """Unit tests for GapDetector — no DB or API."""

    def test_no_gaps_pbm_complete(self):
        """All PBM documents uploaded via merger results → 0 blocking gaps, can_run_comparison=True."""
        docs = _complete_uploaded_documents()
        plan = _complete_plan_parameters()

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        assert len(report.blocking_gaps) == 0
        assert report.can_run_comparison is True
        assert report.engagement_id == _TEST_COMPANY_ID
        assert report.completion_pct > 0

    def test_blocking_gap_missing_balance_sheet(self):
        """Balance sheet MergedParseResult is None → total_liabilities in blocking_gaps."""
        docs = _complete_uploaded_documents()
        docs["balance_sheet"] = None  # Not uploaded
        plan = _complete_plan_parameters()

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        blocking_fields = [g.field for g in report.blocking_gaps]
        assert "total_liabilities" in blocking_fields
        assert "balance_sheet" in report.missing_documents

    def test_blocking_gap_missing_contribution(self):
        """plan_parameters has no total_contribution → blocking gap detected."""
        docs = _complete_uploaded_documents()
        plan = {"practitioner_fee_pct": 10.0}  # Missing total_contribution

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        blocking_fields = [g.field for g in report.blocking_gaps]
        assert "total_contribution" in blocking_fields

    def test_advisory_gap_uncategorised_creditor(self):
        """Creditor with no category → advisory gap (not blocking)."""
        docs = _complete_uploaded_documents()
        # Remove creditor category data
        docs["aged_payables"] = _make_merged("aged_payables", {
            "creditors": [
                {"creditor_name": "ATO", "amount_claimed": 573230.31},
            ],
            "total_claims": 573230.31,
            # creditor[*].category is NOT present → advisory gap
            "creditor[*].is_related_party": [False],
        })
        plan = _complete_plan_parameters()

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        advisory_fields = [g.field for g in report.advisory_gaps]
        assert "creditor[*].category" in advisory_fields
        # Should NOT be in blocking
        blocking_fields = [g.field for g in report.blocking_gaps]
        assert "creditor[*].category" not in blocking_fields

    def test_low_confidence_flagged(self):
        """Field with confidence 0.4 → appears in low_confidence_fields."""
        docs = _complete_uploaded_documents()
        # Override closing_balance with low confidence
        docs["bank_statement"] = _make_merged(
            "bank_statement",
            {"closing_balance": 59689.27, "period_end_date": "2025-12-31"},
            confidence={"closing_balance": 0.4, "period_end_date": 0.95},
        )
        plan = _complete_plan_parameters()

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        low_conf_fields = [g.field for g in report.low_confidence_fields]
        assert "closing_balance" in low_conf_fields
        # Check severity is low_confidence
        closing_gap = next(
            g for g in report.low_confidence_fields if g.field == "closing_balance"
        )
        assert closing_gap.severity == "low_confidence"
        assert closing_gap.current_confidence == 0.4

    def test_can_run_comparison_false(self):
        """Blocking gaps present → can_run_comparison=False."""
        docs = _complete_uploaded_documents()
        docs["balance_sheet"] = None
        plan = _complete_plan_parameters()

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        assert report.can_run_comparison is False
        assert detector.can_run_comparison(report) is False

    def test_director_questionnaire_format(self):
        """Director questionnaire → only director-facing questions, grouped by topic."""
        docs = _complete_uploaded_documents()
        docs["balance_sheet"] = None  # Creates blocking gaps
        docs["bank_statement"] = None  # Creates more gaps
        plan = {"practitioner_fee_pct": 10.0}  # Missing total_contribution

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)
        questions = detector.get_director_questionnaire(report)

        assert len(questions) > 0

        # All questions should have director-facing text
        for q in questions:
            assert q.question is not None
            assert len(q.question) > 0
            assert q.topic in ("financial", "creditors", "operations")

        # practitioner_fee_pct should NOT appear (director=None)
        question_fields = [q.field for q in questions]
        assert "practitioner_fee_pct" not in question_fields

        # Verify ordering: financial before creditors before operations
        topics = [q.topic for q in questions]
        topic_first_idx = {}
        for i, t in enumerate(topics):
            if t not in topic_first_idx:
                topic_first_idx[t] = i
        if "financial" in topic_first_idx and "creditors" in topic_first_idx:
            assert topic_first_idx["financial"] < topic_first_idx["creditors"]
        if "creditors" in topic_first_idx and "operations" in topic_first_idx:
            assert topic_first_idx["creditors"] < topic_first_idx["operations"]

        # Orders should be sequential starting from 1
        orders = [q.order for q in questions]
        assert orders == list(range(1, len(questions) + 1))

    def test_completion_percentage(self):
        """3 of 5 required fields present → completion_pct = 60.0."""
        # We need to carefully craft exactly 5 required fields with 3 present
        # Total required fields across all doc types:
        # aged_payables blocking: creditors, total_claims (2)
        # aged_payables advisory: creditor[*].category, creditor[*].is_related_party (2)
        # balance_sheet blocking: total_liabilities (1)
        # balance_sheet advisory: assets, asset[*].recovery_pct (2)
        # bank_statement blocking: closing_balance (1)
        # bank_statement advisory: period_end_date (1)
        # plan_parameters blocking: total_contribution, practitioner_fee_pct (2)
        # plan_parameters advisory: num_initial_payments, initial_payment_amount (2)
        # Total = 13 required fields

        # Let's make exactly 3 of the fields missing to test a specific percentage
        # For a simple test: set most fields present, remove some
        docs: dict[str, MergedParseResult | None] = {
            "aged_payables": _make_merged("aged_payables", {
                "creditors": [{"creditor_name": "ATO", "amount_claimed": 100}],
                "total_claims": 100,
                "creditor[*].category": ["ato"],
                "creditor[*].is_related_party": [False],
            }),
            "balance_sheet": _make_merged("balance_sheet", {
                "total_liabilities": 985777.37,
                "assets": [{"book_value": 100}],
                "asset[*].recovery_pct": [0.2],
            }),
            "bank_statement": None,  # Missing → 2 fields gone (closing_balance, period_end_date)
            "pnl": None,
        }
        plan = _complete_plan_parameters()  # 4 fields present

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        # 11 present out of 13 total = 84.6%
        assert report.completion_pct == 84.6

    def test_missing_documents_reported(self):
        """Bank statement not uploaded → 'bank_statement' in missing_documents."""
        docs = _complete_uploaded_documents()
        docs["bank_statement"] = None
        plan = _complete_plan_parameters()

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)

        assert "bank_statement" in report.missing_documents

    def test_practitioner_checklist(self):
        """Practitioner checklist includes all gaps, ordered by severity."""
        docs = _complete_uploaded_documents()
        docs["balance_sheet"] = None
        docs["bank_statement"] = None
        plan = _complete_plan_parameters()

        report = detector.detect(_TEST_COMPANY_ID, docs, plan)
        checklist = detector.get_practitioner_checklist(report)

        assert len(checklist) > 0
        # All items have instructions
        for item in checklist:
            assert len(item.instruction) > 0
            assert item.severity in ("blocking", "advisory", "low_confidence")

        # Blocking should come before advisory
        severities = [item.severity for item in checklist]
        if "blocking" in severities and "advisory" in severities:
            last_blocking = max(
                i for i, s in enumerate(severities) if s == "blocking"
            )
            first_advisory = min(
                i for i, s in enumerate(severities) if s == "advisory"
            )
            assert last_blocking < first_advisory

        # Orders should be sequential
        orders = [item.order for item in checklist]
        assert orders == list(range(1, len(checklist) + 1))


# ---------------------------------------------------------------------------
# API Integration Tests (with DB mocking)
# ---------------------------------------------------------------------------


_API_COMPANY_ID = uuid.uuid4()

_PBM_CREDITORS = [
    _FakeCreditor(
        company_id=_API_COMPANY_ID,
        creditor_name="Australian Taxation Office - ITA",
        amount_claimed=573230.31,
        category="ato_ita",
    ),
    _FakeCreditor(
        company_id=_API_COMPANY_ID,
        creditor_name="Trade Creditor A",
        amount_claimed=50000.0,
        category="trade",
    ),
]

_PBM_ASSETS = [
    _FakeAsset(
        company_id=_API_COMPANY_ID,
        asset_type="cash",
        description="Cash at Bank",
        book_value=59689.27,
        liquidation_recovery_pct=0.20,
        liquidation_value=59689.27 * 0.20,
    ),
    _FakeAsset(
        company_id=_API_COMPANY_ID,
        asset_type="receivables",
        description="Accounts Receivable",
        book_value=69553.24,
        liquidation_recovery_pct=0.30,
        liquidation_value=69553.24 * 0.30,
    ),
]

_PBM_PLAN = _FakePlan(company_id=_API_COMPANY_ID)
_PBM_COMPANY = _FakeCompany(
    id=_API_COMPANY_ID,
    legal_name="PBM Holdings Pty Ltd",
    total_creditors=985777.37,
    total_liabilities=985777.37,
)


def _mock_db_execute_full(company, creditors, assets, plan, gap_fills=None):
    """Return a mock execute that returns appropriate results for each query."""
    call_count = [0]

    async def _execute(stmt):
        call_count[0] += 1
        stmt_str = str(stmt)

        # Determine which table is being queried
        if "gap_fills" in stmt_str:
            if gap_fills:
                return _FakeScalarResult(gap_fills)
            return _FakeScalarResult(None)
        elif "companies" in stmt_str:
            return _FakeScalarResult(company)
        elif "creditors" in stmt_str:
            return _FakeScalarResult(creditors)
        elif "assets" in stmt_str:
            return _FakeScalarResult(assets)
        elif "plan_parameters" in stmt_str:
            return _FakeScalarResult(plan)
        else:
            return _FakeScalarResult(None)

    return _execute


@pytest.mark.asyncio
async def test_gap_fill_endpoint():
    """POST /gaps/fill with valid value → gap resolved, gap_fills table updated."""
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute_full(
            _PBM_COMPANY, _PBM_CREDITORS, _PBM_ASSETS, _PBM_PLAN
        )
    )
    mock_db.add = AsyncMock()
    mock_db.flush = AsyncMock()

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _override_get_db

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_API_COMPANY_ID}/gaps/fill",
                json={
                    "field": "closing_balance",
                    "document_type": "bank_statement",
                    "value": 59689.27,
                    "filled_by": "practitioner",
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "blocking_gaps" in data
        assert "can_run_comparison" in data

        # Verify db.add was called (gap_fill was written)
        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_comparison_blocked_by_gaps():
    """POST /compare with blocking gaps → 422 with gap list (not 500)."""
    # Company with no balance sheet total_liabilities → creates blocking gap
    company_no_bs = _FakeCompany(
        id=_API_COMPANY_ID,
        legal_name="Incomplete Co",
        total_creditors=985777.37,
        total_liabilities=0,  # Missing → blocking gap
    )

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute_full(
            company_no_bs,
            _PBM_CREDITORS,
            [],  # No assets → blocking gap on balance_sheet
            _PBM_PLAN,
        )
    )

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _override_get_db

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_API_COMPANY_ID}/compare"
            )
        # Must be 422, NOT 500
        assert resp.status_code == 422
        data = resp.json()
        assert data["error"] == "blocking_gaps"
        assert "blocking_gaps" in data
        assert len(data["blocking_gaps"]) > 0
        assert "hint" in data
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_director_questionnaire_api_format():
    """GET /gaps/questionnaire → only director-facing questions, grouped by topic."""
    # Company with some missing data to generate gaps
    company_incomplete = _FakeCompany(
        id=_API_COMPANY_ID,
        legal_name="Incomplete Co",
        total_creditors=985777.37,
        total_liabilities=0,  # Will trigger total_liabilities gap
    )

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute_full(
            company_incomplete,
            _PBM_CREDITORS,
            [],  # No assets
            None,  # No plan → triggers plan gaps
        )
    )

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _override_get_db

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_API_COMPANY_ID}/gaps/questionnaire"
            )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

        # All entries should have director-facing questions (no None questions)
        for item in data:
            assert "question" in item
            assert item["question"] is not None
            assert len(item["question"]) > 0
            assert item["topic"] in ("financial", "creditors", "operations")

        # practitioner_fee_pct should not appear (director question is None)
        fields = [item["field"] for item in data]
        assert "practitioner_fee_pct" not in fields
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_gap_fill_creates_audit_trail():
    """Fill a gap → previous_value stored in gap_fills row."""
    # Simulate a previous fill exists
    previous_fill = _FakeGapFill(
        engagement_id=_API_COMPANY_ID,
        field_name="closing_balance",
        document_type="bank_statement",
        filled_value=50000.0,
    )

    mock_db = AsyncMock()

    call_count = [0]

    async def _execute(stmt):
        call_count[0] += 1
        stmt_str = str(stmt)

        if "gap_fills" in stmt_str:
            return _FakeScalarResult(previous_fill)
        elif "companies" in stmt_str:
            return _FakeScalarResult(_PBM_COMPANY)
        elif "creditors" in stmt_str:
            return _FakeScalarResult(_PBM_CREDITORS)
        elif "assets" in stmt_str:
            return _FakeScalarResult(_PBM_ASSETS)
        elif "plan_parameters" in stmt_str:
            return _FakeScalarResult(_PBM_PLAN)
        else:
            return _FakeScalarResult(None)

    mock_db.execute = AsyncMock(side_effect=_execute)
    mock_db.add = AsyncMock()
    mock_db.flush = AsyncMock()

    added_objects = []
    original_add = mock_db.add

    def _capture_add(obj):
        added_objects.append(obj)
        return original_add(obj)

    mock_db.add = _capture_add

    async def _override_get_db():
        yield mock_db

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _override_get_db

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_API_COMPANY_ID}/gaps/fill",
                json={
                    "field": "closing_balance",
                    "document_type": "bank_statement",
                    "value": 59689.27,
                    "filled_by": "practitioner",
                },
            )
        assert resp.status_code == 200

        # Verify the GapFillDB object was created with previous_value
        assert len(added_objects) == 1
        gap_fill_obj = added_objects[0]
        assert gap_fill_obj.previous_value == 50000.0
        assert gap_fill_obj.filled_value == 59689.27
        assert gap_fill_obj.filled_by == "practitioner"
    finally:
        app.dependency_overrides.clear()
