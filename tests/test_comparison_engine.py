"""Tests for services.comparison_engine — ComparisonEngine."""

from __future__ import annotations

from pathlib import Path

import pytest

from services.comparison_engine import ComparisonEngine
from services.file_parser import FileParser

FIXTURES = Path(__file__).parent / "fixtures"

engine = ComparisonEngine()
parser = FileParser()


# ---------------------------------------------------------------------------
#  PBM reference data
# ---------------------------------------------------------------------------

# Total creditor claims from balance sheet total_liabilities
PBM_CREDITORS_TOTAL = 985_777.37

# PBM plan parameters
PBM_PLAN = {
    "total_contribution": 516_000.0,
    "practitioner_fee_pct": 10.0,
    "num_initial_payments": 2,
    "initial_payment_amount": 32_500.0,
    "num_ongoing_payments": 22,
    "ongoing_payment_amount": 20_500.0,
    "est_liquidator_fees": 50_000.0,
    "est_legal_fees": 10_000.0,
    "est_disbursements": 5_000.0,
}

# PBM assets — manually constructed with proper type separation
# (shareholder loans separated from related-entity loans for accurate recovery)
PBM_ASSETS = [
    {
        "asset_type": "cash",
        "description": "Cash at Bank",
        "book_value": 59_689.27,
        "liquidation_recovery_pct": 0.20,
        "liquidation_value": 59_689.27 * 0.20,
    },
    {
        "asset_type": "receivables",
        "description": "Accounts Receivable",
        "book_value": 69_553.24,
        "liquidation_recovery_pct": 0.30,
        "liquidation_value": 69_553.24 * 0.30,
    },
    {
        "asset_type": "inventory",
        "description": "Inventory",
        "book_value": 51_826.62,
        "liquidation_recovery_pct": 0.25,
        "liquidation_value": 51_826.62 * 0.25,
    },
    {
        "asset_type": "loans_related",
        "description": "Loans to Related Entities",
        "book_value": 34_964.83,
        "liquidation_recovery_pct": 0.30,
        "liquidation_value": 34_964.83 * 0.30,
    },
    {
        "asset_type": "loans_shareholder",
        "description": "Shareholder Loans",
        "book_value": 2_010_000.00,
        "liquidation_recovery_pct": 0.00,
        "liquidation_value": 0.0,
    },
    {
        "asset_type": "equipment",
        "description": "Plant & Equipment",
        "book_value": 15_000.00,
        "liquidation_recovery_pct": 0.25,
        "liquidation_value": 15_000.00 * 0.25,
    },
]


def _pbm_result() -> dict:
    return engine.calculate(PBM_ASSETS, PBM_CREDITORS_TOTAL, PBM_PLAN)


# ---------------------------------------------------------------------------
#  SBR Scenario Tests
# ---------------------------------------------------------------------------


class TestPBMSBRDividend:
    """SBR dividend = 47.1 cents in the dollar for PBM data."""

    def test_pbm_sbr_dividend(self):
        result = _pbm_result()
        assert result["sbr_dividend_cents"] == 47.1

    def test_pbm_sbr_available(self):
        result = _pbm_result()
        assert result["sbr_available"] == pytest.approx(464_400.0)


# ---------------------------------------------------------------------------
#  Liquidation Scenario Tests
# ---------------------------------------------------------------------------


class TestPBMLiquidationDividend:
    """Liquidation dividend = 0 cents when fees exceed recovered assets."""

    def test_pbm_liquidation_dividend(self):
        # Total recovery at default rates:
        #   cash 11937.85 + receivables 20865.97 + inventory 12956.66
        #   + loans_related 10489.45 + loans_shareholder 0 + equipment 3750.00
        #   = 59999.93
        # Fees: 50000 + 10000 + 5000 = 65000
        # Available = max(0, 59999.93 - 65000) = 0
        result = _pbm_result()
        assert result["liquidation_dividend_cents"] == 0.0

    def test_liquidation_available_is_zero(self):
        result = _pbm_result()
        assert result["liquidation_available"] == 0.0


class TestLiquidationFeesExceedRecovery:
    """When fees > recovered assets, available = 0, dividend = 0."""

    def test_fees_exceed_recovery(self):
        assets = [
            {
                "asset_type": "cash",
                "description": "Cash",
                "book_value": 10_000.0,
                "liquidation_recovery_pct": 0.20,
                "liquidation_value": 2_000.0,
            },
        ]
        plan = {
            "total_contribution": 100_000.0,
            "practitioner_fee_pct": 10.0,
            "est_liquidator_fees": 50_000.0,
            "est_legal_fees": 10_000.0,
            "est_disbursements": 5_000.0,
        }
        result = engine.calculate(assets, 500_000.0, plan)
        assert result["liquidation_available"] == 0.0
        assert result["liquidation_dividend_cents"] == 0.0


# ---------------------------------------------------------------------------
#  Configurable Recovery Rates
# ---------------------------------------------------------------------------


class TestAllRatesConfigurable:
    """Changing recovery rates produces different results."""

    def test_custom_cash_recovery_50pct(self):
        assets = [
            {
                "asset_type": "cash",
                "description": "Cash at Bank",
                "book_value": 100_000.0,
                "liquidation_recovery_pct": 0.50,
                "liquidation_value": 50_000.0,
            },
        ]
        plan = {
            "total_contribution": 100_000.0,
            "practitioner_fee_pct": 10.0,
            "est_liquidator_fees": 10_000.0,
            "est_legal_fees": 5_000.0,
            "est_disbursements": 2_000.0,
        }
        result = engine.calculate(assets, 100_000.0, plan)
        # Liquidation: 50000 - 17000 = 33000
        assert result["liquidation_available"] == pytest.approx(33_000.0)
        # SBR: 100000 - 10000 = 90000
        assert result["sbr_available"] == pytest.approx(90_000.0)

    def test_default_vs_custom_rate_differ(self):
        base_asset = {
            "asset_type": "cash",
            "description": "Cash",
            "book_value": 200_000.0,
        }
        plan = {
            "total_contribution": 100_000.0,
            "practitioner_fee_pct": 10.0,
            "est_liquidator_fees": 10_000.0,
            "est_legal_fees": 5_000.0,
            "est_disbursements": 2_000.0,
        }
        # Default rate 20%
        asset_default = {
            **base_asset,
            "liquidation_recovery_pct": 0.20,
            "liquidation_value": 40_000.0,
        }
        # Custom rate 50%
        asset_custom = {
            **base_asset,
            "liquidation_recovery_pct": 0.50,
            "liquidation_value": 100_000.0,
        }
        r1 = engine.calculate([asset_default], 500_000.0, plan)
        r2 = engine.calculate([asset_custom], 500_000.0, plan)
        assert r1["liquidation_available"] != r2["liquidation_available"]


# ---------------------------------------------------------------------------
#  Zero Asset Liquidation
# ---------------------------------------------------------------------------


class TestZeroAssetLiquidation:
    """All assets have book_value=0 -> dividend=0."""

    def test_zero_assets(self):
        assets = [
            {
                "asset_type": "cash",
                "description": "Cash",
                "book_value": 0.0,
                "liquidation_recovery_pct": 0.20,
                "liquidation_value": 0.0,
            },
            {
                "asset_type": "equipment",
                "description": "Equipment",
                "book_value": 0.0,
                "liquidation_recovery_pct": 0.25,
                "liquidation_value": 0.0,
            },
        ]
        plan = {
            "total_contribution": 100_000.0,
            "practitioner_fee_pct": 10.0,
            "est_liquidator_fees": 50_000.0,
            "est_legal_fees": 10_000.0,
            "est_disbursements": 5_000.0,
        }
        result = engine.calculate(assets, 500_000.0, plan)
        assert result["liquidation_dividend_cents"] == 0.0
        assert result["liquidation_available"] == 0.0


# ---------------------------------------------------------------------------
#  build_assets_from_balance_sheet
# ---------------------------------------------------------------------------


class TestBuildAssetsFromBalanceSheet:
    """Parsed balance sheet -> correct number of asset entries with default rates."""

    def test_build_assets_from_pbm_balance_sheet(self):
        parsed = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assets = engine.build_assets_from_balance_sheet(parsed)
        # Should have 5 asset entries (cash, receivables, inventory,
        # loans_to_related->loans_related, equipment) — not total_liabilities
        assert len(assets) == 5

    def test_asset_types_correct(self):
        parsed = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assets = engine.build_assets_from_balance_sheet(parsed)
        types = {a["asset_type"] for a in assets}
        assert types == {"cash", "receivables", "inventory", "loans_related", "equipment"}

    def test_default_recovery_rates_applied(self):
        parsed = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assets = engine.build_assets_from_balance_sheet(parsed)
        by_type = {a["asset_type"]: a for a in assets}
        assert by_type["cash"]["liquidation_recovery_pct"] == 0.20
        assert by_type["receivables"]["liquidation_recovery_pct"] == 0.30
        assert by_type["inventory"]["liquidation_recovery_pct"] == 0.25
        assert by_type["loans_related"]["liquidation_recovery_pct"] == 0.30
        assert by_type["equipment"]["liquidation_recovery_pct"] == 0.25

    def test_liquidation_values_calculated(self):
        parsed = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assets = engine.build_assets_from_balance_sheet(parsed)
        by_type = {a["asset_type"]: a for a in assets}
        assert by_type["cash"]["liquidation_value"] == pytest.approx(
            59_689.27 * 0.20
        )
        assert by_type["receivables"]["liquidation_value"] == pytest.approx(
            69_553.24 * 0.30
        )

    def test_source_is_parsed(self):
        parsed = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assets = engine.build_assets_from_balance_sheet(parsed)
        for a in assets:
            assert a["source"] == "parsed"

    def test_zero_book_value_excluded(self):
        parsed = {"cash": 0.0, "receivables": 100.0, "inventory": 0.0}
        assets = engine.build_assets_from_balance_sheet(parsed)
        assert len(assets) == 1
        assert assets[0]["asset_type"] == "receivables"


# ---------------------------------------------------------------------------
#  Notes Generation
# ---------------------------------------------------------------------------


class TestNotesGenerated:
    """Comparison result has at least 5 numbered notes."""

    def test_notes_count(self):
        result = _pbm_result()
        assert len(result["notes"]) >= 5

    def test_notes_are_numbered(self):
        result = _pbm_result()
        for note in result["notes"]:
            assert note.startswith("Note ")

    def test_total_creditor_claims_in_result(self):
        result = _pbm_result()
        assert result["total_creditor_claims"] == PBM_CREDITORS_TOTAL
