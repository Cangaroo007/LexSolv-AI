"""
LexSolv AI — Narrative Generator: 6-section Company Offer Statement drafting.

Orchestrates the pipeline:
    Director notes (raw text)
        -> PrivacyVault.scrub() -> scrubbed text + entity map
        -> NarrativeGenerator.generate_section(...)
            -> builds system prompt with glossary + section instructions
            -> ClaudeClient.generate(system_prompt, user_prompt)
        -> PrivacyVault.restore(response, entity_map)
        -> Stored in NarrativeDB with metadata

WARNING: All text containing PII must be scrubbed via
services.privacy_vault.scrub() BEFORE passing to any generate_ method.
The caller is responsible for scrubbing — this service assumes input
has already been de-identified.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from services.claude_client import ClaudeClient, GenerateResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Glossary directory
# ---------------------------------------------------------------------------
GLOSSARY_DIR = Path(__file__).resolve().parent.parent / "data" / "glossaries"

# ---------------------------------------------------------------------------
# System prompt base template
# ---------------------------------------------------------------------------
SYSTEM_PROMPT_BASE = """You are an experienced Australian insolvency professional drafting \
a Company Offer Statement under Part 5.3B of the Corporations Act 2001 (Cth).

Writing style:
- Formal third-person prose (never first person)
- Australian English spelling (organisation, authorised, practise as verb)
- Past tense for events, present tense for current state
- Professional but readable — suitable for creditors who may not be insolvency experts
- Reference specific dates and amounts where available
- Do not invent facts — if information is insufficient, insert [REQUIRES INPUT: brief description of what's needed]

Glossary — use these terms correctly:
{glossary_terms}

If you encounter a technical term not in the glossary above, wrap it as \
[UNKNOWN TERM: the term] so the practitioner can review it.

{section_specific_instructions}"""

# ---------------------------------------------------------------------------
# Section-specific instruction blocks
# ---------------------------------------------------------------------------

SECTION_INSTRUCTIONS = {
    "background": (
        "You are drafting Section I (Background) of the Company Offer Statement.\n\n"
        "Cover the following in formal third-person prose:\n"
        "- Company formation and date of incorporation\n"
        "- Principal business activities\n"
        "- Director details and roles\n"
        "- Trading history and key business relationships\n\n"
        "If the director notes do not provide specific information (e.g. date of "
        "incorporation), insert [REQUIRES INPUT: description of what is needed].\n\n"
        "Do not include financial distress events — those belong in Section II "
        "(Distress Events)."
    ),
    "distress_events": (
        "You are drafting the 'Events Leading to Financial Distress' section.\n\n"
        "Extract and formalise the key distress events from the director's account:\n"
        "- Present events in chronological order\n"
        "- Include approximate dates where provided\n"
        "- Describe the causal chain (e.g. loss of key contract -> cash flow decline "
        "-> creditor arrears)\n"
        "- Reference specific financial impacts where figures are available\n"
        "- Maintain a factual, non-judgmental tone\n\n"
        "Do not attribute blame or make legal conclusions."
    ),
    "expert_advice": (
        "You are drafting Section II (Expert Advice and Appointment) of the "
        "Company Offer Statement.\n\n"
        "This section is largely template-based. Cover:\n"
        "- When the company first sought professional advice\n"
        "- Appointment of the restructuring practitioner (name, date, capacity)\n"
        "- Statutory obligations under Part 5.3B of the Corporations Act 2001\n"
        "- The practitioner's role and responsibilities\n\n"
        "Use the engagement data provided — do not rely on director notes for "
        "this section."
    ),
    "plan_summary": (
        "You are drafting Section III (Restructuring Plan Summary) of the "
        "Company Offer Statement.\n\n"
        "Describe the proposed restructuring plan terms including:\n"
        "- Total contribution amount and payment structure\n"
        "- Practitioner fee percentage and amount\n"
        "- Net amount available for distribution to creditors\n"
        "- Estimated dividend (cents in the dollar)\n"
        "- Payment timeline (initial and ongoing instalments)\n"
        "- Comparison with the liquidation alternative\n\n"
        "Reference specific dollar amounts from the plan parameters. "
        "Use the comparison data to highlight the relative benefit of the SBR plan."
    ),
    "viability": (
        "You are drafting Section IV (Viability Assessment) — 'What Has Changed' "
        "— of the Company Offer Statement.\n\n"
        "Based on the director's explanation, describe:\n"
        "- Operational changes already implemented or planned\n"
        "- New revenue sources or cost reductions\n"
        "- Steps taken to address the causes of financial distress\n"
        "- Why the company is now in a position to trade profitably\n\n"
        "If the director notes do not adequately explain what has changed, "
        "insert [REQUIRES INPUT: explanation of operational changes / new revenue "
        "sources / cost reductions that support ongoing viability]."
    ),
    "comparison_commentary": (
        "You are drafting Section V (Comparison Commentary) — commentary on the "
        "SBR vs Liquidation comparison table (Annexure G).\n\n"
        "Provide a plain-English explanation of:\n"
        "- The key figures from the comparison table\n"
        "- Why the SBR plan provides a better return to creditors\n"
        "- The estimated dividend under each scenario\n"
        "- Any significant assumptions or notes\n\n"
        "Reference specific figures from the comparison data. For example: "
        "'Under the proposed plan, creditors would receive X.X cents in the dollar "
        "compared with Y.Y cents in the dollar (or nil return) in a liquidation.'\n\n"
        "Keep the tone objective and factual."
    ),
}


# ---------------------------------------------------------------------------
# Metadata / flag extraction helpers
# ---------------------------------------------------------------------------

_REQUIRES_INPUT_RE = re.compile(r"\[REQUIRES INPUT:\s*([^\]]+)\]")
_UNKNOWN_TERM_RE = re.compile(r"\[UNKNOWN TERM:\s*([^\]]+)\]")


def _extract_flags(text: str) -> tuple[list[str], list[str]]:
    """Extract [REQUIRES INPUT] and [UNKNOWN TERM] flags from generated text."""
    requires_input = _REQUIRES_INPUT_RE.findall(text)
    unknown_terms = _UNKNOWN_TERM_RE.findall(text)
    return requires_input, unknown_terms


def _sha256(text: str) -> str:
    """Return SHA-256 hex digest of input text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# NarrativeGenerator
# ---------------------------------------------------------------------------

class NarrativeGenerator:
    """
    Generates the six sections of a Company Offer Statement using Claude,
    with glossary injection and source tracking.

    The caller is responsible for:
    1. Scrubbing PII from director notes via privacy_vault.scrub()
    2. Passing scrubbed text to the generate_ methods
    3. Restoring PII in the output via privacy_vault.restore()
    """

    def __init__(
        self,
        claude_client: Optional[ClaudeClient] = None,
        industry: Optional[str] = None,
        custom_terms: Optional[dict[str, str]] = None,
    ) -> None:
        self._client = claude_client or ClaudeClient()
        self._industry = industry
        self._custom_terms = custom_terms or {}
        self._glossary_text: Optional[str] = None

    # ------------------------------------------------------------------
    # Glossary loading
    # ------------------------------------------------------------------

    def _load_glossaries(
        self,
        industry: Optional[str] = None,
        custom_terms: Optional[dict[str, str]] = None,
    ) -> str:
        """
        Load and merge glossary layers for prompt injection.

        Layer 1: Always load insolvency terms.
        Layer 2: Load industry glossary if specified (e.g. "medical").
        Layer 3: Merge any custom client-specific terms.

        Returns formatted string for injection into the system prompt.
        """
        merged: dict[str, str] = {}

        # Layer 1: Insolvency (always active)
        layer1_path = GLOSSARY_DIR / "insolvency_layer1.json"
        if layer1_path.exists():
            with open(layer1_path) as f:
                data = json.load(f)
            merged.update(data.get("terms", {}))
        else:
            logger.warning("Insolvency glossary not found at %s", layer1_path)

        # Layer 2: Industry-specific
        ind = industry or self._industry
        if ind:
            layer2_path = GLOSSARY_DIR / f"{ind}_layer2.json"
            if layer2_path.exists():
                with open(layer2_path) as f:
                    data = json.load(f)
                merged.update(data.get("terms", {}))
            else:
                logger.warning("Industry glossary '%s' not found at %s", ind, layer2_path)

        # Layer 3: Custom terms
        terms = custom_terms or self._custom_terms
        if terms:
            merged.update(terms)

        # Format as bullet list
        lines = [f"- {term}: {definition}" for term, definition in merged.items()]
        return "\n".join(lines)

    def _get_glossary_text(self) -> str:
        """Cache and return the glossary text."""
        if self._glossary_text is None:
            self._glossary_text = self._load_glossaries()
        return self._glossary_text

    def _get_glossary_layers(self) -> list[str]:
        """Return list of active glossary layer names."""
        layers = ["insolvency"]
        if self._industry:
            layers.append(self._industry)
        if self._custom_terms:
            layers.append("custom")
        return layers

    # ------------------------------------------------------------------
    # System prompt builder
    # ------------------------------------------------------------------

    def _build_system_prompt(self, section: str) -> str:
        """Build the full system prompt for a given section."""
        glossary_text = self._get_glossary_text()
        instructions = SECTION_INSTRUCTIONS.get(section, "")
        return SYSTEM_PROMPT_BASE.format(
            glossary_terms=glossary_text,
            section_specific_instructions=instructions,
        )

    # ------------------------------------------------------------------
    # Metadata builder
    # ------------------------------------------------------------------

    def _build_metadata(
        self,
        section: str,
        input_source: str,
        input_text: str,
        result: GenerateResult,
        generated_text: str,
    ) -> dict[str, Any]:
        """Build source tracking metadata for a generated section."""
        requires_input, unknown_terms = _extract_flags(generated_text)
        return {
            "section": section,
            "input_source": input_source,
            "input_hash": _sha256(input_text),
            "glossary_layers": self._get_glossary_layers(),
            "unknown_terms_flagged": unknown_terms,
            "requires_input_flags": [
                f"[REQUIRES INPUT: {flag}]" for flag in requires_input
            ],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model": result.model,
            "token_usage": {
                "input": result.input_tokens,
                "output": result.output_tokens,
            },
        }

    # ------------------------------------------------------------------
    # Section generators
    # ------------------------------------------------------------------

    async def generate_background(
        self,
        scrubbed_director_notes: str,
        engagement_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate Section I (Background) of the Company Offer Statement.

        Parameters
        ----------
        scrubbed_director_notes : str
            Director notes with PII already scrubbed.
        engagement_data : dict
            Engagement context (company name, directors, etc.).

        Returns
        -------
        dict with keys: content, metadata
        """
        system_prompt = self._build_system_prompt("background")
        user_prompt = (
            f"Company context:\n{json.dumps(engagement_data, default=str)}\n\n"
            f"Director notes:\n{scrubbed_director_notes}\n\n"
            "Draft Section I (Background) of the Company Offer Statement."
        )

        result = await self._client.generate(system_prompt, user_prompt)
        metadata = self._build_metadata(
            "background", "director_notes", scrubbed_director_notes, result, result.text
        )
        return {"content": result.text, "metadata": metadata}

    async def generate_distress_events(
        self,
        scrubbed_director_notes: str,
        engagement_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate the 'Events Leading to Financial Distress' narrative.

        Parameters
        ----------
        scrubbed_director_notes : str
            Director notes with PII already scrubbed.
        engagement_data : dict
            Engagement context.

        Returns
        -------
        dict with keys: content, metadata
        """
        system_prompt = self._build_system_prompt("distress_events")
        user_prompt = (
            f"Company context:\n{json.dumps(engagement_data, default=str)}\n\n"
            f"Director notes:\n{scrubbed_director_notes}\n\n"
            "Draft the Events Leading to Financial Distress section."
        )

        result = await self._client.generate(system_prompt, user_prompt)
        metadata = self._build_metadata(
            "distress_events", "director_notes", scrubbed_director_notes, result, result.text
        )
        return {"content": result.text, "metadata": metadata}

    async def generate_expert_advice(
        self,
        engagement_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate Section II (Expert Advice and Appointment).

        Largely template-based — uses engagement data, not director notes.

        Parameters
        ----------
        engagement_data : dict
            Must include: appointment_date, practitioner_name, company details.

        Returns
        -------
        dict with keys: content, metadata
        """
        system_prompt = self._build_system_prompt("expert_advice")
        engagement_str = json.dumps(engagement_data, default=str)
        user_prompt = (
            f"Engagement data:\n{engagement_str}\n\n"
            "Draft Section II (Expert Advice and Appointment) of the "
            "Company Offer Statement. Use the engagement data above — "
            "do not invent additional facts."
        )

        result = await self._client.generate(system_prompt, user_prompt)
        metadata = self._build_metadata(
            "expert_advice", "engagement_data", engagement_str, result, result.text
        )
        return {"content": result.text, "metadata": metadata}

    async def generate_plan_summary(
        self,
        engagement_data: dict[str, Any],
        comparison_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate Section III (Restructuring Plan Summary).

        Parameters
        ----------
        engagement_data : dict
            Plan parameters (contribution, fee%, payment structure).
        comparison_data : dict
            Structured output from ComparisonEngine.calculate().

        Returns
        -------
        dict with keys: content, metadata
        """
        system_prompt = self._build_system_prompt("plan_summary")
        combined_input = json.dumps(
            {"engagement": engagement_data, "comparison": comparison_data},
            default=str,
        )
        user_prompt = (
            f"Engagement and plan data:\n"
            f"{json.dumps(engagement_data, default=str)}\n\n"
            f"Comparison data (SBR vs Liquidation):\n"
            f"{json.dumps(comparison_data, default=str)}\n\n"
            "Draft Section III (Restructuring Plan Summary) of the "
            "Company Offer Statement. Reference specific dollar amounts "
            "and the comparison figures."
        )

        result = await self._client.generate(system_prompt, user_prompt)
        metadata = self._build_metadata(
            "plan_summary", "engagement_data", combined_input, result, result.text
        )
        return {"content": result.text, "metadata": metadata}

    async def generate_viability(
        self,
        scrubbed_director_notes: str,
        engagement_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate Section IV (Viability Assessment) — 'What Has Changed'.

        Parameters
        ----------
        scrubbed_director_notes : str
            Director notes with PII already scrubbed.
        engagement_data : dict
            Engagement context.

        Returns
        -------
        dict with keys: content, metadata
        """
        system_prompt = self._build_system_prompt("viability")
        user_prompt = (
            f"Company context:\n{json.dumps(engagement_data, default=str)}\n\n"
            f"Director notes:\n{scrubbed_director_notes}\n\n"
            "Draft Section IV (Viability Assessment) — explain what has changed "
            "and why the company is now viable."
        )

        result = await self._client.generate(system_prompt, user_prompt)
        metadata = self._build_metadata(
            "viability", "director_notes", scrubbed_director_notes, result, result.text
        )
        return {"content": result.text, "metadata": metadata}

    async def generate_comparison_commentary(
        self,
        comparison_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate Section V (Comparison Commentary) on Annexure G.

        Parameters
        ----------
        comparison_data : dict
            Structured output from ComparisonEngine.calculate().

        Returns
        -------
        dict with keys: content, metadata
        """
        system_prompt = self._build_system_prompt("comparison_commentary")
        comparison_str = json.dumps(comparison_data, default=str)
        user_prompt = (
            f"SBR vs Liquidation comparison data:\n{comparison_str}\n\n"
            "Draft Section V — a plain-English commentary on the comparison table "
            "(Annexure G). Reference specific figures from the data above."
        )

        result = await self._client.generate(system_prompt, user_prompt)
        metadata = self._build_metadata(
            "comparison_commentary", "comparison_data", comparison_str, result, result.text
        )
        return {"content": result.text, "metadata": metadata}
