"""Tests for SBR-specific PII scrubbing in services.privacy_vault.

Verifies that the narrative scrub/restore functions correctly handle
director addresses, client names, trust names, counterparty companies,
bank account numbers, and provider numbers — while preserving dollar
amounts, dates, percentages, and medical/industry terminology.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from services.privacy_vault import (
    DeIdentifier,
    SBRCategory,
    ScrubResult,
    re_identify,
    restore,
    scrub,
)

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
#  Fixture: PBM-style director narrative
# ---------------------------------------------------------------------------

@pytest.fixture()
def narrative_text() -> str:
    """Load the realistic PBM director narrative fixture."""
    return (FIXTURES / "pbm_director_narrative.txt").read_text()


@pytest.fixture()
def known_entities() -> dict[str, list[str]]:
    """Known PII entities that appear in the narrative fixture."""
    return {
        "client_name": ["Dr James Mitchell", "Dr Mitchell"],
        "director_address": ["42 Harbour Road, Manly NSW 2095"],
        "counterparty": [
            "BTC Health Australia",
            "BlueShak Pty Ltd",
            "Prospa Advance",
        ],
        "trust_name": ["Mitchell Family Trust"],
    }


@pytest.fixture()
def scrub_result(narrative_text: str, known_entities: dict) -> ScrubResult:
    """Run scrub() once and share the result across tests."""
    return scrub(narrative_text, known_entities=known_entities)


# ---------------------------------------------------------------------------
#  1. scrub() removes all director/client names → replaced with tags
# ---------------------------------------------------------------------------

class TestScrubNames:
    def test_full_name_scrubbed(self, scrub_result: ScrubResult) -> None:
        assert "Dr James Mitchell" not in scrub_result.scrubbed_text

    def test_short_name_scrubbed(self, scrub_result: ScrubResult) -> None:
        assert "Dr Mitchell" not in scrub_result.scrubbed_text

    def test_name_tags_present(self, scrub_result: ScrubResult) -> None:
        assert "[CLIENT_NAME_A]" in scrub_result.scrubbed_text
        assert "[CLIENT_NAME_B]" in scrub_result.scrubbed_text


# ---------------------------------------------------------------------------
#  2. scrub() removes all addresses → replaced with tags
# ---------------------------------------------------------------------------

class TestScrubAddresses:
    def test_address_scrubbed(self, scrub_result: ScrubResult) -> None:
        assert "42 Harbour Road" not in scrub_result.scrubbed_text
        assert "Manly NSW 2095" not in scrub_result.scrubbed_text

    def test_address_tag_present(self, scrub_result: ScrubResult) -> None:
        assert "[DIRECTOR_ADDRESS_A]" in scrub_result.scrubbed_text


# ---------------------------------------------------------------------------
#  3. scrub() removes counterparty company names → replaced with tags
# ---------------------------------------------------------------------------

class TestScrubCounterparties:
    def test_btc_health_scrubbed(self, scrub_result: ScrubResult) -> None:
        assert "BTC Health Australia" not in scrub_result.scrubbed_text

    def test_blueshak_scrubbed(self, scrub_result: ScrubResult) -> None:
        assert "BlueShak Pty Ltd" not in scrub_result.scrubbed_text

    def test_prospa_scrubbed(self, scrub_result: ScrubResult) -> None:
        assert "Prospa Advance" not in scrub_result.scrubbed_text

    def test_counterparty_tags_present(self, scrub_result: ScrubResult) -> None:
        assert "[COUNTERPARTY_A]" in scrub_result.scrubbed_text
        assert "[COUNTERPARTY_B]" in scrub_result.scrubbed_text
        assert "[COUNTERPARTY_C]" in scrub_result.scrubbed_text


# ---------------------------------------------------------------------------
#  4. scrub() preserves dollar amounts exactly
# ---------------------------------------------------------------------------

class TestPreserveDollarAmounts:
    def test_preserve_573k(self, scrub_result: ScrubResult) -> None:
        assert "$573,230.31" in scrub_result.scrubbed_text

    def test_preserve_516k(self, scrub_result: ScrubResult) -> None:
        assert "$516,000" in scrub_result.scrubbed_text

    def test_preserve_87500(self, scrub_result: ScrubResult) -> None:
        assert "$87,500" in scrub_result.scrubbed_text

    def test_preserve_142k(self, scrub_result: ScrubResult) -> None:
        assert "$142,000" in scrub_result.scrubbed_text

    def test_preserve_210k(self, scrub_result: ScrubResult) -> None:
        assert "$210,000" in scrub_result.scrubbed_text

    def test_preserve_48k_monthly(self, scrub_result: ScrubResult) -> None:
        assert "$48,000" in scrub_result.scrubbed_text

    def test_preserve_12k_monthly(self, scrub_result: ScrubResult) -> None:
        assert "$12,000" in scrub_result.scrubbed_text

    def test_preserve_185k(self, scrub_result: ScrubResult) -> None:
        assert "$185,000" in scrub_result.scrubbed_text

    def test_preserve_412k(self, scrub_result: ScrubResult) -> None:
        assert "$412,000" in scrub_result.scrubbed_text


# ---------------------------------------------------------------------------
#  5. scrub() preserves dates exactly
# ---------------------------------------------------------------------------

class TestPreserveDates:
    def test_preserve_march_date(self, scrub_result: ScrubResult) -> None:
        assert "15 March 2024" in scrub_result.scrubbed_text

    def test_preserve_july_date(self, scrub_result: ScrubResult) -> None:
        assert "1 July 2023" in scrub_result.scrubbed_text

    def test_preserve_june_date(self, scrub_result: ScrubResult) -> None:
        assert "30 June 2023" in scrub_result.scrubbed_text


# ---------------------------------------------------------------------------
#  6. scrub() preserves medical/industry terms
# ---------------------------------------------------------------------------

class TestPreserveMedicalTerms:
    def test_preserve_orthopaedic(self, scrub_result: ScrubResult) -> None:
        assert "orthopaedic" in scrub_result.scrubbed_text

    def test_preserve_allograft_tissue(self, scrub_result: ScrubResult) -> None:
        assert "allograft tissue" in scrub_result.scrubbed_text

    def test_preserve_surgical_consumables(self, scrub_result: ScrubResult) -> None:
        assert "surgical consumables" in scrub_result.scrubbed_text

    def test_preserve_titanium_fixation(self, scrub_result: ScrubResult) -> None:
        assert "titanium fixation devices" in scrub_result.scrubbed_text

    def test_preserve_surgical_mesh(self, scrub_result: ScrubResult) -> None:
        assert "surgical mesh products" in scrub_result.scrubbed_text


# ---------------------------------------------------------------------------
#  7. restore() with entity_map produces original text exactly
# ---------------------------------------------------------------------------

class TestRestore:
    def test_restore_produces_original(
        self, narrative_text: str, scrub_result: ScrubResult
    ) -> None:
        restored = restore(scrub_result.scrubbed_text, scrub_result.entity_map)
        assert restored == narrative_text

    def test_restore_replaces_all_tags(self, scrub_result: ScrubResult) -> None:
        restored = restore(scrub_result.scrubbed_text, scrub_result.entity_map)
        # No SBR tags should remain after restoration
        for tag in scrub_result.entity_map:
            assert tag not in restored


# ---------------------------------------------------------------------------
#  8. Round-trip: restore(scrub(text)) == original text
# ---------------------------------------------------------------------------

class TestRoundTrip:
    def test_round_trip_fidelity(
        self, narrative_text: str, known_entities: dict
    ) -> None:
        result = scrub(narrative_text, known_entities=known_entities)
        restored = restore(result.scrubbed_text, result.entity_map)
        assert restored == narrative_text

    def test_round_trip_with_short_text(self) -> None:
        text = "Dr Smith lives at 10 King Street, Sydney NSW 2000 and runs Smith Family Trust."
        entities = {"client_name": ["Dr Smith"]}
        result = scrub(text, known_entities=entities)
        restored = restore(result.scrubbed_text, result.entity_map)
        assert restored == text


# ---------------------------------------------------------------------------
#  9. Multiple entities of same type get sequential tags (A, B, C)
# ---------------------------------------------------------------------------

class TestSequentialTags:
    def test_counterparty_sequential(self, scrub_result: ScrubResult) -> None:
        """Three counterparties → [COUNTERPARTY_A], [COUNTERPARTY_B], [COUNTERPARTY_C]."""
        tags = [t for t in scrub_result.entity_map if t.startswith("[COUNTERPARTY_")]
        assert len(tags) >= 3
        assert "[COUNTERPARTY_A]" in tags
        assert "[COUNTERPARTY_B]" in tags
        assert "[COUNTERPARTY_C]" in tags

    def test_client_name_sequential(self, scrub_result: ScrubResult) -> None:
        """Two name variants → [CLIENT_NAME_A], [CLIENT_NAME_B]."""
        tags = [t for t in scrub_result.entity_map if t.startswith("[CLIENT_NAME_")]
        assert len(tags) == 2
        assert "[CLIENT_NAME_A]" in tags
        assert "[CLIENT_NAME_B]" in tags

    def test_letter_sequence_helper(self) -> None:
        """Verify _seq_letter generates correct sequences."""
        from services.privacy_vault import _seq_letter

        assert _seq_letter(0) == "A"
        assert _seq_letter(1) == "B"
        assert _seq_letter(25) == "Z"
        assert _seq_letter(26) == "AA"
        assert _seq_letter(27) == "AB"


# ---------------------------------------------------------------------------
# 10. New categories don't break existing PII categories
# ---------------------------------------------------------------------------

class TestExistingCategoriesUnaffected:
    def test_deidentifier_still_works(self) -> None:
        """Existing DeIdentifier handles structured JSON as before."""
        engine = DeIdentifier()
        data = {
            "contact_name": "Alice Johnson",
            "email_address": "alice@example.com",
            "phone_number": "0412 345 678",
            "address": "99 George St, Sydney NSW 2000",
            "amount": 5000.00,
            "date": "2024-01-15",
        }
        result = engine.de_identify(data)

        # Sensitive fields tokenized
        assert result.sanitized_data["contact_name"] != "Alice Johnson"
        assert result.sanitized_data["email_address"] != "alice@example.com"
        assert result.sanitized_data["phone_number"] != "0412 345 678"
        assert result.sanitized_data["address"] != "99 George St, Sydney NSW 2000"

        # Non-sensitive fields preserved
        assert result.sanitized_data["amount"] == 5000.00
        assert result.sanitized_data["date"] == "2024-01-15"

    def test_deidentifier_re_identify_round_trip(self) -> None:
        """DeIdentifier → re_identify round trip still works."""
        engine = DeIdentifier()
        data = {"contact_name": "Bob Smith", "amount": 1234.56}
        result = engine.de_identify(data)
        restored = re_identify(result.sanitized_data, result.vault_id)
        assert restored["contact_name"] == "Bob Smith"
        assert restored["amount"] == 1234.56

    def test_sensitive_field_category_unchanged(self) -> None:
        """Original SensitiveFieldCategory enum has exactly 7 members."""
        from services.privacy_vault import SensitiveFieldCategory

        assert len(SensitiveFieldCategory) == 7
        expected = {"name", "address", "email", "phone", "abn", "bank_account", "tax_number"}
        assert {c.value for c in SensitiveFieldCategory} == expected


# ---------------------------------------------------------------------------
# 11. Zero raw PII in scrubbed output (negative assertion — Rule 18)
# ---------------------------------------------------------------------------

class TestNoPIILeak:
    """Ensure no raw PII appears anywhere in scrubbed output."""

    RAW_PII = [
        "Dr James Mitchell",
        "Dr Mitchell",
        "42 Harbour Road",
        "Manly NSW 2095",
        "BTC Health Australia",
        "BlueShak Pty Ltd",
        "Prospa Advance",
        "Mitchell Family Trust",
        "062-000 Account 12345678",
        "2834710F",
    ]

    def test_no_raw_pii_in_scrubbed_text(self, scrub_result: ScrubResult) -> None:
        for pii in self.RAW_PII:
            assert pii not in scrub_result.scrubbed_text, (
                f"Raw PII leaked into scrubbed output: {pii!r}"
            )

    def test_entity_map_contains_all_pii(self, scrub_result: ScrubResult) -> None:
        """Every known PII value should appear in the entity_map values."""
        map_values = set(scrub_result.entity_map.values())
        # These are the known-entity PII that must be captured
        required = {
            "Dr James Mitchell",
            "Dr Mitchell",
            "42 Harbour Road, Manly NSW 2095",
            "BTC Health Australia",
            "BlueShak Pty Ltd",
            "Prospa Advance",
            "Mitchell Family Trust",
        }
        for pii in required:
            assert pii in map_values, f"Entity map missing PII: {pii!r}"


# ---------------------------------------------------------------------------
# Bonus: Bank account and provider number regex detection
# ---------------------------------------------------------------------------

class TestRegexDetection:
    def test_bsb_account_detected(self, scrub_result: ScrubResult) -> None:
        """BSB + account number detected by regex."""
        assert "062-000 Account 12345678" not in scrub_result.scrubbed_text
        bank_tags = [t for t in scrub_result.entity_map if "BANK_ACCOUNT" in t]
        assert len(bank_tags) >= 1

    def test_provider_number_detected(self, scrub_result: ScrubResult) -> None:
        """Provider number detected by regex."""
        assert "2834710F" not in scrub_result.scrubbed_text
        provider_tags = [t for t in scrub_result.entity_map if "PROVIDER_NUMBER" in t]
        assert len(provider_tags) >= 1

    def test_scrub_result_is_dataclass(self, scrub_result: ScrubResult) -> None:
        assert isinstance(scrub_result, ScrubResult)
        assert isinstance(scrub_result.scrubbed_text, str)
        assert isinstance(scrub_result.entity_map, dict)
