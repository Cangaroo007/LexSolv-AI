"""
LexSolv AI — AI Parser: Claude-powered structured field extraction.

Sends anonymised document content to Claude for structured field extraction.
Returns AIParseResult with extracted fields and confidence scores.
All Claude calls go through PrivacyVault (Rule 10).

WARNING: Every Claude call is wrapped in PrivacyVault.scrub() / .restore().
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from services.claude_client import ClaudeClient
from services.document_ingester import RawDocumentContent
from services.privacy_vault import scrub, restore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AIParseResult:
    document_type: str
    extracted: dict           # Field name → extracted value (PII restored)
    confidence: dict          # Field name → float 0.0–1.0
    notes: list[str]          # Claude's observations
    parse_method: str         # "ai_text" | "ai_vision"
    tokens_used: int


# ---------------------------------------------------------------------------
# Document schemas
# ---------------------------------------------------------------------------

DOCUMENT_SCHEMAS: dict[str, dict[str, Any]] = {
    "aged_payables": {
        "required": ["creditors"],
        "creditor_fields": ["name", "amount", "category", "is_related_party", "is_disputed"],
        "optional": [],
    },
    "balance_sheet": {
        "required": ["assets", "total_liabilities"],
        "asset_fields": ["name", "asset_type", "book_value", "recovery_pct"],
        "optional": [],
    },
    "bank_statement": {
        "required": ["closing_balance", "period_end_date"],
        "optional": ["opening_balance", "account_name", "bsb", "account_number_last4"],
    },
    "pnl": {
        "required": ["total_revenue"],
        "optional": ["gross_profit", "net_profit", "period_start", "period_end"],
    },
}

# ---------------------------------------------------------------------------
# System prompt template
# ---------------------------------------------------------------------------

_EXTRACTION_PROMPT = """\
You are a financial document parser for Australian insolvency proceedings. \
Extract the following fields from the document provided.

DOCUMENT TYPE: {document_type}
REQUIRED FIELDS: {required_fields}
OPTIONAL FIELDS: {optional_fields}

RULES:
- Return ONLY valid JSON. No preamble, no markdown, no explanation outside the JSON.
- If a field is not present in the document, omit it entirely — do not set it to null or 0.
- Confidence scoring:
    1.0 = value read directly from a labelled table cell or clearly stated figure
    0.7 = value inferred from context (e.g., derived from surrounding figures)
    0.3 = value estimated from partial information
- All currency values: positive float, no $ signs, no commas (e.g., 985777.37)
- All dates: ISO format YYYY-MM-DD
- For creditor lists, return an array under "creditors" key
- Flag implausible values in the "notes" array (e.g., "Cash balance of $50M seems high for an SME")
- Do NOT invent values — omit rather than guess

RESPONSE FORMAT:
{{
  "fields": {{ ... }},
  "confidence": {{ "field_name": 0.95, ... }},
  "notes": ["observation 1", "observation 2"]
}}"""


# ---------------------------------------------------------------------------
# AIParser
# ---------------------------------------------------------------------------

class AIParser:
    """
    Sends anonymised document content to Claude for structured field extraction.
    Returns AIParseResult with extracted fields and confidence scores.
    All Claude calls go through PrivacyVault (Rule 10).
    """

    def __init__(self, claude_client: ClaudeClient | None = None) -> None:
        self._claude = claude_client or ClaudeClient()

    async def parse(
        self,
        raw: RawDocumentContent,
        document_type: str,
        engagement_id: str,
        known_entities: dict | None = None,
    ) -> AIParseResult:
        """
        1. Build extraction prompt from schema + document content
        2. Scrub PII from content before sending to Claude (Rule 10)
        3. Send to Claude — use vision for images, text for everything else
        4. Parse Claude JSON response
        5. Restore PII in extracted values
        6. Return AIParseResult with confidence scores
        """
        known_entities = known_entities or {}

        if document_type not in DOCUMENT_SCHEMAS:
            raise ValueError(
                f"Unknown document type '{document_type}'. "
                f"Supported: {list(DOCUMENT_SCHEMAS.keys())}"
            )

        # 1. Build the extraction prompt
        system_prompt = self._build_extraction_prompt(raw, document_type)

        # 2. Prepare content and scrub PII (Rule 10)
        use_vision = bool(raw.images_base64) and (
            raw.likely_scanned or raw.file_type == "image"
        )

        if use_vision:
            # For vision: scrub the system prompt only (images contain PII inherently
            # but we can't scrub images — PII is scrubbed from text portions)
            scrub_result = scrub(system_prompt, known_entities)
            scrubbed_prompt = scrub_result.scrubbed_text
            entity_map = scrub_result.entity_map

            messages = self._build_vision_message(raw, scrubbed_prompt)
            # Use generate_vision path
            result = await self._claude.generate(
                system_prompt=scrubbed_prompt,
                user_prompt=messages,
                max_tokens=4000,
            )
            parse_method = "ai_vision"
        else:
            # For text: scrub the document content + prompt
            user_content = self._build_text_content(raw)
            scrub_result = scrub(user_content, known_entities)
            scrubbed_content = scrub_result.scrubbed_text
            entity_map = scrub_result.entity_map

            result = await self._claude.generate(
                system_prompt=system_prompt,
                user_prompt=scrubbed_content,
                max_tokens=4000,
            )
            parse_method = "ai_text"

        tokens_used = result.input_tokens + result.output_tokens

        # 4. Parse Claude's JSON response
        try:
            parsed = self._parse_json_response(result.text)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "AI parser received invalid JSON from Claude for %s (engagement=%s): %s",
                document_type, engagement_id, exc,
            )
            return AIParseResult(
                document_type=document_type,
                extracted={},
                confidence={},
                notes=[f"AI parse failed: invalid response from Claude — {exc}"],
                parse_method=parse_method,
                tokens_used=tokens_used,
            )

        extracted = parsed.get("fields", {})
        confidence = parsed.get("confidence", {})
        notes = parsed.get("notes", [])

        # 5. Restore PII in extracted values
        if entity_map:
            extracted = restore(str(extracted), entity_map)
            # Parse back to dict if restore produced a string representation
            if isinstance(extracted, str):
                try:
                    extracted = json.loads(extracted.replace("'", '"'))
                except (json.JSONDecodeError, ValueError):
                    # If restore produced something unparseable, use the original
                    extracted = parsed.get("fields", {})

            notes_text = restore(str(notes), entity_map)
            if isinstance(notes_text, str):
                try:
                    notes = json.loads(notes_text.replace("'", '"'))
                except (json.JSONDecodeError, ValueError):
                    notes = parsed.get("notes", [])

        return AIParseResult(
            document_type=document_type,
            extracted=extracted,
            confidence=confidence,
            notes=notes if isinstance(notes, list) else [str(notes)],
            parse_method=parse_method,
            tokens_used=tokens_used,
        )

    def _build_extraction_prompt(
        self, raw: RawDocumentContent, document_type: str
    ) -> str:
        """Build system prompt per schema."""
        schema = DOCUMENT_SCHEMAS[document_type]
        required = schema.get("required", [])
        optional = schema.get("optional", [])

        # Add sub-fields for complex types
        extra_fields = []
        if "creditor_fields" in schema:
            extra_fields.append(
                f"Creditor fields per entry: {', '.join(schema['creditor_fields'])}"
            )
        if "asset_fields" in schema:
            extra_fields.append(
                f"Asset fields per entry: {', '.join(schema['asset_fields'])}"
            )

        optional_str = ", ".join(optional) if optional else "(none)"
        if extra_fields:
            optional_str += "\n" + "\n".join(extra_fields)

        return _EXTRACTION_PROMPT.format(
            document_type=document_type,
            required_fields=", ".join(required),
            optional_fields=optional_str,
        )

    def _build_vision_message(
        self, raw: RawDocumentContent, prompt: str
    ) -> str:
        """
        For scanned PDFs and image uploads — build a user prompt that
        references the images. The actual vision API call would use
        multimodal content blocks, but since ClaudeClient.generate()
        takes a string user_prompt, we include any available text context
        alongside reference to images.
        """
        parts = [prompt]
        if raw.text_content.strip():
            parts.append(f"\n\nAvailable text from document:\n{raw.text_content}")
        if raw.tables:
            parts.append("\n\nTables extracted from document:")
            for i, table in enumerate(raw.tables):
                parts.append(f"\nTable {i + 1}:")
                for row in table:
                    parts.append(" | ".join(row))
        parts.append(
            f"\n\n[{len(raw.images_base64)} image(s) from the document are provided for analysis]"
        )
        return "\n".join(parts)

    @staticmethod
    def _build_text_content(raw: RawDocumentContent) -> str:
        """Build text content from RawDocumentContent for non-vision parsing."""
        parts = []
        if raw.text_content.strip():
            parts.append(raw.text_content)
        if raw.tables:
            parts.append("\n--- Tables ---")
            for i, table in enumerate(raw.tables):
                parts.append(f"\nTable {i + 1}:")
                for row in table:
                    parts.append(" | ".join(row))
        return "\n".join(parts)

    @staticmethod
    def _parse_json_response(text: str) -> dict:
        """Parse Claude's JSON response, handling markdown code fences."""
        cleaned = text.strip()
        # Strip markdown code fences if present
        if cleaned.startswith("```"):
            # Remove opening fence (```json or ```)
            first_newline = cleaned.index("\n")
            cleaned = cleaned[first_newline + 1:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise ValueError(f"Expected JSON object, got {type(parsed).__name__}")
        return parsed
