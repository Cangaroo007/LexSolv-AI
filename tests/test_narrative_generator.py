"""Tests for the narrative generator pipeline (Prompt 2.2).

Unit tests mock the Claude API — no real API calls.
Integration test requires ANTHROPIC_API_KEY and is skipped if not set.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.claude_client import ClaudeClient, GenerateResult
from services.narrative_generator import (
    GLOSSARY_DIR,
    SYSTEM_PROMPT_BASE,
    NarrativeGenerator,
    _extract_flags,
)
from services.privacy_vault import restore, scrub

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def narrative_text() -> str:
    """Load the PBM director narrative fixture."""
    return (FIXTURES / "pbm_director_narrative.txt").read_text()


@pytest.fixture()
def known_entities() -> dict[str, list[str]]:
    """Known PII entities for scrubbing."""
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
def engagement_data() -> dict:
    """Sample engagement data for narrative generation."""
    return {
        "company_name": "[ENTITY_001]",
        "acn": "123 456 789",
        "appointment_date": "2026-01-15",
        "practitioner_name": "[ENTITY_002]",
        "total_contribution": 210000.0,
        "practitioner_fee_pct": 10.0,
    }


@pytest.fixture()
def comparison_data() -> dict:
    """Sample comparison engine output."""
    return {
        "lines": [
            {"description": "Cash at Bank", "note_number": 1, "sbr_value": None, "liquidation_value": 20000.0},
            {"description": "Available for distribution", "note_number": None, "sbr_value": 189000.0, "liquidation_value": 0.0},
            {"description": "Estimated dividend (cents in the dollar)", "note_number": None, "sbr_value": 47.1, "liquidation_value": 0.0},
        ],
        "notes": ["Note 1: Cash at Bank — book value $100,000.00, estimated recovery rate 20%."],
        "sbr_available": 189000.0,
        "sbr_dividend_cents": 47.1,
        "liquidation_available": 0.0,
        "liquidation_dividend_cents": 0.0,
        "total_creditor_claims": 401200.0,
    }


@pytest.fixture()
def mock_generate_result() -> GenerateResult:
    """A mock GenerateResult from Claude."""
    return GenerateResult(
        text=(
            "The company was incorporated in [REQUIRES INPUT: date of incorporation] "
            "and has traded as a provider of [UNKNOWN TERM: allograft tissue banking] services. "
            "The company operates from its registered office."
        ),
        input_tokens=1500,
        output_tokens=800,
        model="claude-sonnet-4-20250514",
    )


# ---------------------------------------------------------------------------
# 1. test_system_prompt_includes_glossary
# ---------------------------------------------------------------------------

class TestGlossaryInjection:
    def test_system_prompt_includes_glossary(self) -> None:
        """Verify insolvency Layer 1 terms are injected into the system prompt."""
        gen = NarrativeGenerator()
        prompt = gen._build_system_prompt("background")
        assert "SBR" in prompt
        assert "Small Business Restructuring" in prompt
        assert "DIRRI" in prompt
        assert "preferential_payment" in prompt
        assert "relation_back_day" in prompt
        assert "restructuring_practitioner" in prompt

    def test_system_prompt_includes_medical_glossary(self) -> None:
        """Verify Layer 2 medical terms when industry='medical'."""
        gen = NarrativeGenerator(industry="medical")
        prompt = gen._build_system_prompt("background")
        # Layer 1 still present
        assert "SBR" in prompt
        # Layer 2 medical terms
        assert "allograft" in prompt
        assert "orthopaedic" in prompt
        assert "surgical_consumables" in prompt
        assert "TGA" in prompt
        assert "Therapeutic Goods Administration" in prompt

    def test_custom_terms_merge(self) -> None:
        """Verify custom terms are merged into the glossary."""
        custom = {"widget_factor": "A custom term for testing"}
        gen = NarrativeGenerator(custom_terms=custom)
        prompt = gen._build_system_prompt("background")
        assert "widget_factor" in prompt
        assert "A custom term for testing" in prompt
        # Layer 1 still present
        assert "SBR" in prompt

    def test_glossary_layers_tracking(self) -> None:
        """Verify _get_glossary_layers returns correct layer names."""
        gen = NarrativeGenerator()
        assert gen._get_glossary_layers() == ["insolvency"]

        gen2 = NarrativeGenerator(industry="medical")
        assert gen2._get_glossary_layers() == ["insolvency", "medical"]

        gen3 = NarrativeGenerator(industry="medical", custom_terms={"x": "y"})
        assert gen3._get_glossary_layers() == ["insolvency", "medical", "custom"]


# ---------------------------------------------------------------------------
# 2. test_scrub_before_claude / test_restore_after_claude
# ---------------------------------------------------------------------------

class TestPIIPipeline:
    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_scrub_before_claude(
        self,
        mock_generate: AsyncMock,
        narrative_text: str,
        known_entities: dict,
        engagement_data: dict,
    ) -> None:
        """Verify scrub() is called before generate() in the pipeline."""
        mock_generate.return_value = GenerateResult(
            text="The company was incorporated and has traded.",
            input_tokens=100,
            output_tokens=50,
            model="claude-sonnet-4-20250514",
        )

        # Scrub first (as the caller would)
        result = scrub(narrative_text, known_entities=known_entities)

        # Pass scrubbed text to generator
        gen = NarrativeGenerator()
        await gen.generate_background(result.scrubbed_text, engagement_data)

        # Verify mock was called with scrubbed text (no raw PII)
        call_args = mock_generate.call_args
        user_prompt = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("user_prompt", "")
        assert "Dr James Mitchell" not in user_prompt
        assert "42 Harbour Road" not in user_prompt

    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_restore_after_claude(
        self,
        mock_generate: AsyncMock,
        narrative_text: str,
        known_entities: dict,
        engagement_data: dict,
    ) -> None:
        """Verify restore() can reconstruct PII in the Claude response."""
        mock_generate.return_value = GenerateResult(
            text="[CLIENT_NAME_A] operated the practice from [DIRECTOR_ADDRESS_A].",
            input_tokens=100,
            output_tokens=50,
            model="claude-sonnet-4-20250514",
        )

        result = scrub(narrative_text, known_entities=known_entities)
        gen = NarrativeGenerator()
        output = await gen.generate_background(result.scrubbed_text, engagement_data)

        # Restore PII
        restored = restore(output["content"], result.entity_map)
        assert "Dr James Mitchell" in restored
        assert "42 Harbour Road, Manly NSW 2095" in restored

    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_no_raw_pii_in_prompt(
        self,
        mock_generate: AsyncMock,
        narrative_text: str,
        known_entities: dict,
        engagement_data: dict,
    ) -> None:
        """Pass text with PII, verify only scrubbed version sent to mock Claude."""
        mock_generate.return_value = GenerateResult(
            text="Generated narrative text.",
            input_tokens=100,
            output_tokens=50,
            model="claude-sonnet-4-20250514",
        )

        # Scrub PII
        result = scrub(narrative_text, known_entities=known_entities)

        gen = NarrativeGenerator()
        await gen.generate_background(result.scrubbed_text, engagement_data)

        # Check the user_prompt sent to Claude
        call_args = mock_generate.call_args
        user_prompt = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("user_prompt", "")

        raw_pii = [
            "Dr James Mitchell",
            "Dr Mitchell",
            "42 Harbour Road",
            "Manly NSW 2095",
            "BTC Health Australia",
            "BlueShak Pty Ltd",
            "Prospa Advance",
            "Mitchell Family Trust",
        ]
        for pii in raw_pii:
            assert pii not in user_prompt, f"Raw PII found in prompt: {pii!r}"


# ---------------------------------------------------------------------------
# 3. test_requires_input_flags_extracted / test_unknown_terms_extracted
# ---------------------------------------------------------------------------

class TestFlagExtraction:
    def test_requires_input_flags_extracted(self) -> None:
        """Verify [REQUIRES INPUT] tags are parsed from response."""
        text = (
            "The company was incorporated on [REQUIRES INPUT: date of incorporation]. "
            "The director [REQUIRES INPUT: director qualifications] holds various positions."
        )
        requires_input, _ = _extract_flags(text)
        assert len(requires_input) == 2
        assert "date of incorporation" in requires_input
        assert "director qualifications" in requires_input

    def test_unknown_terms_extracted(self) -> None:
        """Verify [UNKNOWN TERM] tags are parsed from response."""
        text = (
            "The company supplied [UNKNOWN TERM: allograft tissue banking] services "
            "and [UNKNOWN TERM: cryogenic preservation] equipment."
        )
        _, unknown_terms = _extract_flags(text)
        assert len(unknown_terms) == 2
        assert "allograft tissue banking" in unknown_terms
        assert "cryogenic preservation" in unknown_terms

    def test_no_flags_when_none_present(self) -> None:
        """Verify empty lists when no flags are present."""
        text = "The company was incorporated in 2018 and has traded successfully."
        requires_input, unknown_terms = _extract_flags(text)
        assert requires_input == []
        assert unknown_terms == []


# ---------------------------------------------------------------------------
# 4. test_source_tracking_metadata
# ---------------------------------------------------------------------------

class TestSourceTracking:
    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_source_tracking_metadata(
        self,
        mock_generate: AsyncMock,
        mock_generate_result: GenerateResult,
        engagement_data: dict,
    ) -> None:
        """Verify metadata dict has all required fields."""
        mock_generate.return_value = mock_generate_result

        gen = NarrativeGenerator(industry="medical")
        output = await gen.generate_background("Scrubbed text here.", engagement_data)

        metadata = output["metadata"]
        assert metadata["section"] == "background"
        assert metadata["input_source"] == "director_notes"
        assert "input_hash" in metadata
        assert len(metadata["input_hash"]) == 64  # SHA-256 hex digest
        assert metadata["glossary_layers"] == ["insolvency", "medical"]
        assert "generated_at" in metadata
        assert metadata["model"] == "claude-sonnet-4-20250514"
        assert metadata["token_usage"]["input"] == 1500
        assert metadata["token_usage"]["output"] == 800

        # Flags extracted from mock response
        assert len(metadata["requires_input_flags"]) == 1
        assert "date of incorporation" in metadata["requires_input_flags"][0]
        assert len(metadata["unknown_terms_flagged"]) == 1
        assert "allograft tissue banking" in metadata["unknown_terms_flagged"][0]


# ---------------------------------------------------------------------------
# 5. test_all_six_sections_callable
# ---------------------------------------------------------------------------

class TestAllSectionsCallable:
    def test_all_six_sections_callable(self) -> None:
        """Verify each generate method exists and is callable."""
        gen = NarrativeGenerator()
        methods = [
            gen.generate_background,
            gen.generate_distress_events,
            gen.generate_expert_advice,
            gen.generate_plan_summary,
            gen.generate_viability,
            gen.generate_comparison_commentary,
        ]
        for method in methods:
            assert callable(method), f"{method.__name__} is not callable"

    def test_section_instructions_cover_all_sections(self) -> None:
        """Verify SECTION_INSTRUCTIONS has entries for all 6 sections."""
        from services.narrative_generator import SECTION_INSTRUCTIONS

        expected = {
            "background",
            "distress_events",
            "expert_advice",
            "plan_summary",
            "viability",
            "comparison_commentary",
        }
        assert set(SECTION_INSTRUCTIONS.keys()) == expected


# ---------------------------------------------------------------------------
# 6. test_narrative_db_model_creates
# ---------------------------------------------------------------------------

class TestNarrativeDBModel:
    def test_narrative_db_model_creates(self) -> None:
        """Verify NarrativeDB can be instantiated with correct fields."""
        from db.models import NarrativeDB

        narrative = NarrativeDB(
            engagement_id=uuid.uuid4(),
            section="background",
            content="Test content for background section.",
            status="draft",
            metadata_={"section": "background", "model": "claude-sonnet-4-20250514"},
            entity_map={"[CLIENT_NAME_A]": "Test Name"},
        )

        assert narrative.section == "background"
        assert narrative.content == "Test content for background section."
        assert narrative.status == "draft"
        assert narrative.metadata_["model"] == "claude-sonnet-4-20250514"
        assert narrative.entity_map["[CLIENT_NAME_A]"] == "Test Name"

    def test_narrative_db_has_correct_tablename(self) -> None:
        """Verify the NarrativeDB model maps to 'narratives' table."""
        from db.models import NarrativeDB

        assert NarrativeDB.__tablename__ == "narratives"


# ---------------------------------------------------------------------------
# 7. Claude client handles missing API key
# ---------------------------------------------------------------------------

class TestClaudeClientMissingKey:
    def test_missing_api_key_raises(self) -> None:
        """Claude client raises RuntimeError when API key is missing."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove ANTHROPIC_API_KEY if present
            env = os.environ.copy()
            env.pop("ANTHROPIC_API_KEY", None)
            with patch.dict(os.environ, env, clear=True):
                client = ClaudeClient(api_key=None)
                with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
                    client._ensure_client()


# ---------------------------------------------------------------------------
# 8. Pydantic schema validation
# ---------------------------------------------------------------------------

class TestPydanticSchemas:
    def test_narrative_section_schema(self) -> None:
        """Verify NarrativeSection schema instantiation."""
        from models.schemas import NarrativeSection

        section = NarrativeSection(
            section="background",
            content="Test content",
            status="draft",
            metadata_={"model": "test"},
            requires_input_flags=["[REQUIRES INPUT: date]"],
            unknown_terms=["allograft"],
        )
        assert section.section == "background"
        assert section.status == "draft"
        assert len(section.requires_input_flags) == 1

    def test_narrative_response_schema(self) -> None:
        """Verify NarrativeResponse schema instantiation."""
        from models.schemas import NarrativeResponse, NarrativeSection

        resp = NarrativeResponse(
            engagement_id=str(uuid.uuid4()),
            sections=[
                NarrativeSection(section="background", content="Test"),
            ],
            generated_at="2026-02-18T10:30:00Z",
        )
        assert len(resp.sections) == 1
        assert resp.sections[0].section == "background"


# ---------------------------------------------------------------------------
# 9. Each section generator produces correct metadata section name
# ---------------------------------------------------------------------------

class TestSectionMetadata:
    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_distress_events_metadata(
        self, mock_generate: AsyncMock, engagement_data: dict
    ) -> None:
        mock_generate.return_value = GenerateResult(
            text="Events leading to distress.", input_tokens=100, output_tokens=50, model="claude-sonnet-4-20250514"
        )
        gen = NarrativeGenerator()
        output = await gen.generate_distress_events("Scrubbed notes.", engagement_data)
        assert output["metadata"]["section"] == "distress_events"

    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_expert_advice_metadata(
        self, mock_generate: AsyncMock, engagement_data: dict
    ) -> None:
        mock_generate.return_value = GenerateResult(
            text="Expert advice section.", input_tokens=100, output_tokens=50, model="claude-sonnet-4-20250514"
        )
        gen = NarrativeGenerator()
        output = await gen.generate_expert_advice(engagement_data)
        assert output["metadata"]["section"] == "expert_advice"
        assert output["metadata"]["input_source"] == "engagement_data"

    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_plan_summary_metadata(
        self, mock_generate: AsyncMock, engagement_data: dict, comparison_data: dict
    ) -> None:
        mock_generate.return_value = GenerateResult(
            text="Plan summary section.", input_tokens=100, output_tokens=50, model="claude-sonnet-4-20250514"
        )
        gen = NarrativeGenerator()
        output = await gen.generate_plan_summary(engagement_data, comparison_data)
        assert output["metadata"]["section"] == "plan_summary"

    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_viability_metadata(
        self, mock_generate: AsyncMock, engagement_data: dict
    ) -> None:
        mock_generate.return_value = GenerateResult(
            text="Viability section.", input_tokens=100, output_tokens=50, model="claude-sonnet-4-20250514"
        )
        gen = NarrativeGenerator()
        output = await gen.generate_viability("Scrubbed notes.", engagement_data)
        assert output["metadata"]["section"] == "viability"

    @pytest.mark.asyncio
    @patch("services.claude_client.ClaudeClient.generate")
    async def test_comparison_commentary_metadata(
        self, mock_generate: AsyncMock, comparison_data: dict
    ) -> None:
        mock_generate.return_value = GenerateResult(
            text="Commentary section.", input_tokens=100, output_tokens=50, model="claude-sonnet-4-20250514"
        )
        gen = NarrativeGenerator()
        output = await gen.generate_comparison_commentary(comparison_data)
        assert output["metadata"]["section"] == "comparison_commentary"
        assert output["metadata"]["input_source"] == "comparison_data"


# ---------------------------------------------------------------------------
# 10. Integration test — requires ANTHROPIC_API_KEY
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.getenv("ANTHROPIC_API_KEY"),
    reason="No API key — skipping integration test",
)
class TestIntegrationPBMBackground:
    @pytest.mark.asyncio
    async def test_pbm_background_generation(
        self, narrative_text: str, known_entities: dict
    ) -> None:
        """Generate background section from PBM director narrative fixture.

        Verifies:
        - Output is formal third-person prose
        - Output mentions key events (supply agreement termination)
        - No raw PII in output
        """
        # Scrub PII
        result = scrub(narrative_text, known_entities=known_entities)

        engagement_data = {
            "company_name": "[COUNTERPARTY_A]",
            "appointment_date": "2026-01-15",
            "practitioner_name": "[ENTITY_001]",
        }

        gen = NarrativeGenerator(industry="medical")
        output = await gen.generate_background(result.scrubbed_text, engagement_data)

        content = output["content"]

        # Should be formal prose (not empty, reasonable length)
        assert len(content) > 100
        # Should not contain first person
        assert " I " not in content
        assert " my " not in content.lower().split(".")  # crude check

        # No raw PII in output
        assert "Dr James Mitchell" not in content
        assert "42 Harbour Road" not in content
        assert "Manly NSW 2095" not in content

        # Metadata present
        assert output["metadata"]["section"] == "background"
        assert output["metadata"]["model"] == "claude-sonnet-4-20250514"
        assert output["metadata"]["token_usage"]["input"] > 0
        assert output["metadata"]["token_usage"]["output"] > 0
