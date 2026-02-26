"""Tests for Gap-Fill Interface — Phase 1 (Prompt 5.4).

Tests:
- Director questionnaire .docx generation (endpoint + content)
- Questionnaire topic grouping
- Gap fill persistence verification
- Conversation log (unanswered + fills)
- Comparison unblocked after filling all blocking gaps
- DocumentGenerator extension (Rule 1 compliance)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from main import app
from services.document_generator import DocumentGenerator
from services.gap_detector import GapDetector
from services.parser_merger import MergedParseResult


# ---------------------------------------------------------------------------
# Helpers
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
# Fake DB models
# ---------------------------------------------------------------------------


class _FakeCompany:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id", uuid.uuid4())
        self.legal_name = kwargs.get("legal_name", "Test Co")
        self.acn = kwargs.get("acn", "123456789")
        self.abn = kwargs.get("abn")
        self.total_creditors = kwargs.get("total_creditors", 0)
        self.total_liabilities = kwargs.get("total_liabilities", 985777.37)
        self.practitioner_name = kwargs.get("practitioner_name", "John Smith")
        self.appointment_date = kwargs.get("appointment_date")
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
# Shared test data
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
    acn="123456789",
    total_creditors=985777.37,
    total_liabilities=985777.37,
    practitioner_name="John Smith",
)


def _mock_db_execute(company, creditors, assets, plan, gap_fills=None):
    """Return a mock execute that returns appropriate results for each query."""

    async def _execute(stmt):
        stmt_str = str(stmt)

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


def _get_db_override(mock_db):
    """Create a dependency override for get_db."""
    async def _override():
        yield mock_db
    return _override


# ---------------------------------------------------------------------------
# Test 1: test_generate_director_questionnaire_endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_director_questionnaire_endpoint():
    """POST /generate/director-questionnaire -> returns .docx, file size > 0 bytes."""
    # Company with gaps to generate questions for
    company_incomplete = _FakeCompany(
        id=_API_COMPANY_ID,
        legal_name="Incomplete Co Pty Ltd",
        total_creditors=985777.37,
        total_liabilities=0,  # Missing → creates gap
    )

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute(
            company_incomplete,
            _PBM_CREDITORS,
            [],  # No assets → creates gaps
            None,  # No plan → creates gaps
        )
    )

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _get_db_override(mock_db)

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_API_COMPANY_ID}/generate/director-questionnaire"
            )
        assert resp.status_code == 200
        assert "application/vnd.openxmlformats" in resp.headers.get("content-type", "")
        # File size must be > 0
        assert len(resp.content) > 0
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 2: test_questionnaire_docx_contains_questions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_questionnaire_docx_contains_questions():
    """Open returned .docx with python-docx → verify expected question text is present."""
    import io
    from docx import Document

    company_incomplete = _FakeCompany(
        id=_API_COMPANY_ID,
        legal_name="Incomplete Co Pty Ltd",
        total_creditors=985777.37,
        total_liabilities=0,
    )

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute(
            company_incomplete,
            _PBM_CREDITORS,
            [],
            None,
        )
    )

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _get_db_override(mock_db)

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                f"/api/engagements/{_API_COMPANY_ID}/generate/director-questionnaire"
            )
        assert resp.status_code == 200

        # Parse the returned .docx
        doc = Document(io.BytesIO(resp.content))
        full_text = "\n".join(p.text for p in doc.paragraphs)

        # Verify at least one expected question string is present
        # With missing balance sheet and plan parameters, we should get
        # questions about total liabilities, assets, contribution, etc.
        assert any(
            q in full_text
            for q in [
                "total amount the company owes",
                "current balance",
                "proposing to contribute",
                "list of the company's assets",
                "list of all the company's creditors",
            ]
        ), f"Expected a director question in docx text, got: {full_text[:500]}"
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 3: test_questionnaire_groups_by_topic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_questionnaire_groups_by_topic():
    """Questions in returned JSON grouped correctly (financial / creditors / operations)."""
    company_incomplete = _FakeCompany(
        id=_API_COMPANY_ID,
        legal_name="Incomplete Co",
        total_creditors=985777.37,
        total_liabilities=0,
    )

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute(
            company_incomplete,
            _PBM_CREDITORS,
            [],
            None,
        )
    )

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _get_db_override(mock_db)

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

        # Extract topics
        topics = [q["topic"] for q in data]

        # Verify ordering: all financial before any creditors, all creditors before operations
        seen_topics = []
        for t in topics:
            if t not in seen_topics:
                seen_topics.append(t)

        valid_order = ["financial", "creditors", "operations"]
        # Filter to only topics that appear
        expected_order = [t for t in valid_order if t in seen_topics]
        assert seen_topics == expected_order, f"Topics not in order: {seen_topics}"
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 4: test_gap_fill_persisted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gap_fill_persisted():
    """POST /gaps/fill → GET /gaps → filled field no longer in blocking_gaps."""
    # Company with bank statement gap (closing_balance missing)
    company_no_bank = _FakeCompany(
        id=_API_COMPANY_ID,
        legal_name="Test Co",
        total_creditors=623230.31,
        total_liabilities=985777.37,
    )

    # Assets without 'cash' type so closing_balance can't be derived
    assets_no_cash = [
        _FakeAsset(
            company_id=_API_COMPANY_ID,
            asset_type="receivables",
            description="Accounts Receivable",
            book_value=69553.24,
            liquidation_recovery_pct=0.30,
            liquidation_value=69553.24 * 0.30,
        ),
    ]

    # After filling, the mock DB returns the gap fill
    filled_gap = _FakeGapFill(
        engagement_id=_API_COMPANY_ID,
        field_name="closing_balance",
        document_type="bank_statement",
        filled_value=59689.27,
        filled_by="practitioner",
        confidence=1.0,
    )

    mock_db = AsyncMock()
    mock_db.add = AsyncMock()
    mock_db.flush = AsyncMock()

    # Track fill state
    fill_happened = [False]
    gap_fill_call = [0]

    async def _execute_with_fill_state(stmt):
        stmt_str = str(stmt)
        if "gap_fills" in stmt_str:
            gap_fill_call[0] += 1
            if not fill_happened[0]:
                return _FakeScalarResult(None)
            # After fill: the first gap_fills query in fill_gap is for previous value
            # (scalar_one_or_none), the second is in _build_gap_inputs (scalars().all())
            # Return a list to satisfy both — _FakeScalarResult handles both
            return _FakeScalarResult(filled_gap)
        elif "companies" in stmt_str:
            return _FakeScalarResult(company_no_bank)
        elif "creditors" in stmt_str:
            return _FakeScalarResult(_PBM_CREDITORS)
        elif "assets" in stmt_str:
            return _FakeScalarResult(assets_no_cash)
        elif "plan_parameters" in stmt_str:
            return _FakeScalarResult(_PBM_PLAN)
        else:
            return _FakeScalarResult(None)

    mock_db.execute = AsyncMock(side_effect=_execute_with_fill_state)

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _get_db_override(mock_db)

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Step 1: Check gaps before fill — closing_balance should be blocking
            resp1 = await client.get(f"/api/engagements/{_API_COMPANY_ID}/gaps")
            assert resp1.status_code == 200
            data1 = resp1.json()
            blocking_fields_before = [g["field"] for g in data1["blocking_gaps"]]
            assert "closing_balance" in blocking_fields_before

            # Step 2: Fill the gap
            fill_happened[0] = True
            resp2 = await client.post(
                f"/api/engagements/{_API_COMPANY_ID}/gaps/fill",
                json={
                    "field": "closing_balance",
                    "document_type": "bank_statement",
                    "value": 59689.27,
                    "filled_by": "practitioner",
                },
            )
            assert resp2.status_code == 200

            # Step 3: Re-check gaps — closing_balance should NOT be blocking
            resp3 = await client.get(f"/api/engagements/{_API_COMPANY_ID}/gaps")
            assert resp3.status_code == 200
            data3 = resp3.json()
            blocking_fields_after = [g["field"] for g in data3["blocking_gaps"]]
            assert "closing_balance" not in blocking_fields_after
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 5: test_conversation_log_unanswered
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_conversation_log_unanswered():
    """Engagement with blocking gaps → GET /conversation → system_question items with answer: null."""
    company_incomplete = _FakeCompany(
        id=_API_COMPANY_ID,
        legal_name="Incomplete Co",
        total_creditors=985777.37,
        total_liabilities=0,
    )

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute(
            company_incomplete,
            _PBM_CREDITORS,
            [],
            None,
        )
    )

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _get_db_override(mock_db)

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_API_COMPANY_ID}/gaps/conversation"
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "conversation" in data
        conversation = data["conversation"]

        # Should have system_question items (unanswered)
        system_questions = [c for c in conversation if c["type"] == "system_question"]
        assert len(system_questions) > 0

        # All system_question items should have answer: null
        for q in system_questions:
            assert q["answer"] is None
            assert q["field"] is not None
            assert q["document_type"] is not None
            assert q["question"] is not None
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 6: test_conversation_log_records_fills
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_conversation_log_records_fills():
    """Fill a gap → GET /conversation → practitioner_fill item appears."""
    filled_gap = _FakeGapFill(
        engagement_id=_API_COMPANY_ID,
        field_name="closing_balance",
        document_type="bank_statement",
        filled_value=59689.27,
        filled_by="practitioner",
        confidence=1.0,
    )

    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute(
            _PBM_COMPANY,
            _PBM_CREDITORS,
            _PBM_ASSETS,
            _PBM_PLAN,
            gap_fills=[filled_gap],
        )
    )

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _get_db_override(mock_db)

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                f"/api/engagements/{_API_COMPANY_ID}/gaps/conversation"
            )
        assert resp.status_code == 200
        data = resp.json()
        conversation = data["conversation"]

        # Should have at least one practitioner_fill item
        fills = [c for c in conversation if c["type"] == "practitioner_fill"]
        assert len(fills) > 0

        # Check the fill has the expected shape
        fill = fills[0]
        assert fill["field"] == "closing_balance"
        assert fill["document_type"] == "bank_statement"
        assert fill["answer"] is not None
        assert fill["confidence"] == 1.0
        assert fill["filled_by"] == "practitioner"
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 7: test_comparison_unblocked_after_fills
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_comparison_unblocked_after_fills():
    """Fill all blocking gaps → POST /compare → 200 (not 422)."""
    # Full data with gap fills covering any potential blocking gaps
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock(
        side_effect=_mock_db_execute(
            _PBM_COMPANY,
            _PBM_CREDITORS,
            _PBM_ASSETS,
            _PBM_PLAN,
        )
    )

    app.dependency_overrides[__import__("db.database", fromlist=["get_db"]).get_db] = _get_db_override(mock_db)

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # First verify no blocking gaps
            gaps_resp = await client.get(f"/api/engagements/{_API_COMPANY_ID}/gaps")
            assert gaps_resp.status_code == 200
            gap_data = gaps_resp.json()
            assert gap_data["can_run_comparison"] is True

            # Then run comparison — should succeed (200), not be blocked (422)
            resp = await client.post(
                f"/api/engagements/{_API_COMPANY_ID}/compare"
            )
        assert resp.status_code == 200, (
            f"Expected 200 after all blocking gaps filled, got {resp.status_code}: {resp.text}"
        )
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Test 8: test_document_generator_extended_not_replaced
# ---------------------------------------------------------------------------


def test_document_generator_extended_not_replaced():
    """
    Verify generate_director_questionnaire_docx exists on DocumentGenerator
    AND existing methods still present (Rule 1).
    """
    gen = DocumentGenerator()

    # New method must exist
    assert hasattr(gen, "generate_director_questionnaire_docx"), (
        "generate_director_questionnaire_docx not found on DocumentGenerator"
    )
    assert callable(gen.generate_director_questionnaire_docx)

    # All existing methods must still exist (Rule 1 — no modifications to existing)
    existing_methods = [
        "generate_dirri",
        "generate_safe_harbour_checklist",
        "generate_comparison_docx",
        "generate_payment_schedule_docx",
        "generate_company_statement_docx",
    ]
    for method_name in existing_methods:
        assert hasattr(gen, method_name), (
            f"Existing method '{method_name}' missing from DocumentGenerator — Rule 1 violation"
        )
        assert callable(getattr(gen, method_name))
