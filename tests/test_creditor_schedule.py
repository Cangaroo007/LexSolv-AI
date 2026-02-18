"""Tests for services.creditor_schedule — CreditorScheduleService."""

from __future__ import annotations

from pathlib import Path

import pytest

from services.creditor_schedule import CreditorScheduleService
from services.file_parser import FileParser

FIXTURES = Path(__file__).parent / "fixtures"

parser = FileParser()
service = CreditorScheduleService()

# Pre-load the PBM aged payables as parsed dicts
_parsed = parser.parse_aged_payables(str(FIXTURES / "pbm_aged_payables.csv"))


def _build() -> list[dict]:
    """Build a fresh creditor schedule from the PBM fixture."""
    return service.build_from_parsed(_parsed)


def _by_name(creditors: list[dict]) -> dict[str, dict]:
    return {c["creditor_name"]: c for c in creditors}


# ------------------------------------------------------------------ #
#  Classification tests                                               #
# ------------------------------------------------------------------ #


class TestBuildFromParsedClassification:
    def test_build_from_parsed_classifies_ato_ita(self):
        creditors = _by_name(_build())
        c = creditors["Australian Taxation Office - ITA"]
        assert c["category"] == "ato_ita"
        assert c["amount_claimed"] == pytest.approx(573230.31)

    def test_build_from_parsed_classifies_ato_ica(self):
        creditors = _by_name(_build())
        assert creditors["Australian Taxation Office - ICA"]["category"] == "ato_ica"

    def test_build_from_parsed_classifies_workers_comp(self):
        creditors = _by_name(_build())
        assert creditors["iCare NSW"]["category"] == "workers_comp"

    def test_build_from_parsed_classifies_finance(self):
        creditors = _by_name(_build())
        assert creditors["Prospa Advance"]["category"] == "finance"

    def test_build_from_parsed_defaults_to_trade(self):
        creditors = _by_name(_build())
        assert creditors["BlueShak"]["category"] == "trade"
        assert creditors["BTC Health Australia"]["category"] == "trade"


# ------------------------------------------------------------------ #
#  Related-party flagging                                              #
# ------------------------------------------------------------------ #


class TestRelatedPartyFlagging:
    def test_flag_related_party_sets_cannot_vote(self):
        creditors = _build()
        blueshak = next(c for c in creditors if c["creditor_name"] == "BlueShak")
        service.flag_related_party(blueshak, is_related=True)
        assert blueshak["is_related_party"] is True
        assert blueshak["can_vote"] is False

    def test_unflag_related_party_restores_vote(self):
        creditors = _build()
        blueshak = next(c for c in creditors if c["creditor_name"] == "BlueShak")
        service.flag_related_party(blueshak, is_related=True)
        service.flag_related_party(blueshak, is_related=False)
        assert blueshak["is_related_party"] is False
        assert blueshak["can_vote"] is True


# ------------------------------------------------------------------ #
#  Status updates and totals                                          #
# ------------------------------------------------------------------ #


class TestStatusAndTotals:
    def test_forgiven_excluded_from_admitted(self):
        creditors = _build()
        blueshak = next(c for c in creditors if c["creditor_name"] == "BlueShak")
        service.update_status(blueshak, "forgiven")
        totals = service.calculate_totals(creditors)
        # BlueShak's 142105.81 should NOT be in total_admitted
        assert blueshak["amount_claimed"] not in [0.0]
        assert totals["total_admitted"] == pytest.approx(
            573230.31 + 268294.01 + 825.23 + 143874.02 + 67447.99
        )

    def test_disputed_included_in_totals(self):
        creditors = _build()
        btc = next(c for c in creditors if c["creditor_name"] == "BTC Health Australia")
        service.update_status(btc, "disputed")
        totals = service.calculate_totals(creditors)
        # BTC Health's amount should still be in total_claims
        assert totals["total_claims"] == pytest.approx(
            573230.31 + 268294.01 + 825.23 + 143874.02 + 142105.81 + 67447.99
        )

    def test_calculate_totals_pbm(self):
        creditors = _build()
        totals = service.calculate_totals(creditors)
        expected_total = 573230.31 + 268294.01 + 825.23 + 143874.02 + 142105.81 + 67447.99
        assert totals["total_claims"] == pytest.approx(expected_total)


# ------------------------------------------------------------------ #
#  Source field                                                        #
# ------------------------------------------------------------------ #


class TestSourceField:
    def test_all_creditors_start_as_parsed(self):
        creditors = _build()
        for c in creditors:
            assert c["source"] == "parsed"
