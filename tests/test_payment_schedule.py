"""Tests for services.payment_schedule — PaymentScheduleGenerator."""

from __future__ import annotations

import pytest

from services.payment_schedule import PaymentScheduleGenerator

generator = PaymentScheduleGenerator()


# ---------------------------------------------------------------------------
#  PBM reference plan parameters
# ---------------------------------------------------------------------------

PBM_PLAN = {
    "total_contribution": 516_000.0,
    "practitioner_fee_pct": 10.0,
    "num_initial_payments": 2,
    "initial_payment_amount": 32_500.0,
    "num_ongoing_payments": 22,
    "ongoing_payment_amount": 20_500.0,
}


def _pbm_schedule() -> dict:
    return generator.generate(PBM_PLAN)


# ---------------------------------------------------------------------------
#  Schedule Structure
# ---------------------------------------------------------------------------


class TestPBMScheduleCount:
    """24 total entries: 2 initial + 22 ongoing."""

    def test_pbm_schedule_count(self):
        result = _pbm_schedule()
        assert len(result["entries"]) == 24


# ---------------------------------------------------------------------------
#  Initial Payments
# ---------------------------------------------------------------------------


class TestPBMInitialPayments:
    """First 2 entries have total_payment = $32,500 each."""

    def test_pbm_initial_payments(self):
        result = _pbm_schedule()
        for entry in result["entries"][:2]:
            assert entry["total_payment"] == pytest.approx(32_500.0)

    def test_initial_month_labels(self):
        result = _pbm_schedule()
        assert result["entries"][0]["month_label"] == "Month 1"
        assert result["entries"][1]["month_label"] == "Month 2"

    def test_initial_payment_numbers(self):
        result = _pbm_schedule()
        assert result["entries"][0]["payment_number"] == 1
        assert result["entries"][1]["payment_number"] == 2

    def test_initial_fee_breakdown(self):
        result = _pbm_schedule()
        for entry in result["entries"][:2]:
            assert entry["practitioner_fee"] == pytest.approx(3_250.0)
            assert entry["net_dividend"] == pytest.approx(29_250.0)


# ---------------------------------------------------------------------------
#  Ongoing Payments
# ---------------------------------------------------------------------------


class TestPBMOngoingPayments:
    """Entries 3-24 have total_payment = $20,500 each."""

    def test_pbm_ongoing_payments(self):
        result = _pbm_schedule()
        for entry in result["entries"][2:]:
            assert entry["total_payment"] == pytest.approx(20_500.0)

    def test_ongoing_month_labels(self):
        result = _pbm_schedule()
        assert result["entries"][2]["month_label"] == "Month 3"
        assert result["entries"][-1]["month_label"] == "Month 24"

    def test_ongoing_fee_breakdown(self):
        result = _pbm_schedule()
        for entry in result["entries"][2:]:
            assert entry["practitioner_fee"] == pytest.approx(2_050.0)
            assert entry["net_dividend"] == pytest.approx(18_450.0)


# ---------------------------------------------------------------------------
#  Totals
# ---------------------------------------------------------------------------


class TestPBMSumEqualsContribution:
    """Sum of all total_payments = $516,000."""

    def test_pbm_sum_equals_contribution(self):
        result = _pbm_schedule()
        total = sum(e["total_payment"] for e in result["entries"])
        assert total == pytest.approx(516_000.0)


class TestPBMTotalFees:
    """Sum of all practitioner_fee = $51,600."""

    def test_pbm_total_fees(self):
        result = _pbm_schedule()
        total_fees = sum(e["practitioner_fee"] for e in result["entries"])
        assert total_fees == pytest.approx(51_600.0)
        assert result["total_fees"] == pytest.approx(51_600.0)


class TestPBMTotalNetDividend:
    """Sum of all net_dividend = $464,400."""

    def test_pbm_total_net_dividend(self):
        result = _pbm_schedule()
        total_net = sum(e["net_dividend"] for e in result["entries"])
        assert total_net == pytest.approx(464_400.0)
        assert result["total_net_dividend"] == pytest.approx(464_400.0)


# ---------------------------------------------------------------------------
#  Validation
# ---------------------------------------------------------------------------


class TestValidationErrorOnMismatch:
    """Payments don't sum to contribution -> ValueError."""

    def test_validation_error_on_mismatch(self):
        bad_plan = {
            "total_contribution": 500_000.0,  # doesn't match
            "practitioner_fee_pct": 10.0,
            "num_initial_payments": 2,
            "initial_payment_amount": 32_500.0,
            "num_ongoing_payments": 22,
            "ongoing_payment_amount": 20_500.0,
        }
        with pytest.raises(ValueError, match="does not balance"):
            generator.generate(bad_plan)

    def test_validation_includes_discrepancy(self):
        bad_plan = {
            "total_contribution": 100_000.0,
            "practitioner_fee_pct": 10.0,
            "num_initial_payments": 1,
            "initial_payment_amount": 50_000.0,
            "num_ongoing_payments": 1,
            "ongoing_payment_amount": 60_000.0,
        }
        # Sum = 110,000, contribution = 100,000, discrepancy = 10,000
        with pytest.raises(ValueError, match="10,000"):
            generator.generate(bad_plan)
