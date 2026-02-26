"""
LexSolv AI — Parser Merger: combines structured and AI parse results.

Merges results from the structured parser (FileParser) and AI parser (AIParser).

Merge strategy:
- Structured parser wins when is_structured=True AND field confidence >= 0.95
- AI parser fills fields missing from structured result
- When both have a value, take the higher-confidence one
- Flag conflicts (values differ by >5%) for practitioner review

Returns MergedParseResult with source tracking per field.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from services.ai_parser import AIParseResult
from services.document_ingester import RawDocumentContent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ConflictRecord:
    field: str
    structured_value: Any
    ai_value: Any
    structured_confidence: float
    ai_confidence: float


@dataclass
class MergedParseResult:
    document_type: str
    fields: dict                          # Field name → winning value
    confidence: dict[str, float]          # Field name → confidence score
    source: dict[str, str]                # Field name → "structured_parser" | "ai_parser" | "conflict"
    conflicts: list[ConflictRecord]       # Fields where parsers disagreed by >5%
    parse_summary: str                    # e.g. "Parsed via Xero CSV + AI (3 fields AI-filled)"


# Default confidence for structured parser fields
_STRUCTURED_DEFAULT_CONFIDENCE = 0.98
_STRUCTURED_WIN_THRESHOLD = 0.95
_CONFLICT_THRESHOLD = 0.05  # 5% difference triggers a conflict flag


# ---------------------------------------------------------------------------
# ParserMerger
# ---------------------------------------------------------------------------

class ParserMerger:
    """
    Merges results from the structured parser (FileParser) and AI parser (AIParser).

    Merge strategy:
    - Structured parser wins when is_structured=True AND field confidence >= 0.95
    - AI parser fills fields missing from structured result
    - When both have a value, take the higher-confidence one
    - Flag conflicts (values differ by >5%) for practitioner review

    Returns MergedParseResult with source tracking per field.
    """

    def merge(
        self,
        structured: dict | None,
        ai_result: AIParseResult | None,
        raw: RawDocumentContent,
    ) -> MergedParseResult:
        """
        For each field in the merged output:
        - value: the winning value
        - confidence: float
        - source: "structured_parser" | "ai_parser" | "merged" | "conflict"
        - conflict: dict with both values if they disagree
        """
        document_type = ai_result.document_type if ai_result else "unknown"
        fields: dict = {}
        confidence: dict[str, float] = {}
        source: dict[str, str] = {}
        conflicts: list[ConflictRecord] = []
        summary_parts: list[str] = []

        # --- Structured-only path ---
        if structured is not None and ai_result is None:
            flat = self._flatten_structured(structured)
            for key, value in flat.items():
                fields[key] = value
                confidence[key] = _STRUCTURED_DEFAULT_CONFIDENCE
                source[key] = "structured_parser"
            summary_parts.append("Parsed via structured parser only")
            return MergedParseResult(
                document_type=document_type,
                fields=fields,
                confidence=confidence,
                source=source,
                conflicts=conflicts,
                parse_summary=" — ".join(summary_parts),
            )

        # --- AI-only path ---
        if structured is None and ai_result is not None:
            for key, value in ai_result.extracted.items():
                fields[key] = value
                confidence[key] = ai_result.confidence.get(key, 0.5)
                source[key] = "ai_parser"
            summary_parts.append(
                f"Parsed via AI ({ai_result.parse_method})"
            )
            return MergedParseResult(
                document_type=ai_result.document_type,
                fields=fields,
                confidence=confidence,
                source=source,
                conflicts=conflicts,
                parse_summary=" — ".join(summary_parts),
            )

        # --- Both None ---
        if structured is None and ai_result is None:
            return MergedParseResult(
                document_type="unknown",
                fields={},
                confidence={},
                source={},
                conflicts=[],
                parse_summary="No parse results available",
            )

        # --- Merge path: both structured and AI results ---
        flat_structured = self._flatten_structured(structured)
        ai_fields = ai_result.extracted
        ai_confidence = ai_result.confidence

        all_keys = set(flat_structured.keys()) | set(ai_fields.keys())
        structured_count = 0
        ai_filled_count = 0
        conflict_count = 0

        for key in all_keys:
            s_val = flat_structured.get(key)
            a_val = ai_fields.get(key)
            s_conf = _STRUCTURED_DEFAULT_CONFIDENCE if key in flat_structured else 0.0
            a_conf = ai_confidence.get(key, 0.5) if key in ai_fields else 0.0

            if s_val is not None and a_val is None:
                # Only structured has it
                fields[key] = s_val
                confidence[key] = s_conf
                source[key] = "structured_parser"
                structured_count += 1

            elif s_val is None and a_val is not None:
                # Only AI has it — fills the gap
                fields[key] = a_val
                confidence[key] = a_conf
                source[key] = "ai_parser"
                ai_filled_count += 1

            else:
                # Both have a value — check for conflict
                is_conflict = self._values_conflict(s_val, a_val)

                if is_conflict:
                    conflicts.append(ConflictRecord(
                        field=key,
                        structured_value=s_val,
                        ai_value=a_val,
                        structured_confidence=s_conf,
                        ai_confidence=a_conf,
                    ))
                    # Still pick a winner: structured wins if high confidence
                    if raw.is_structured and s_conf >= _STRUCTURED_WIN_THRESHOLD:
                        fields[key] = s_val
                        confidence[key] = s_conf
                    elif a_conf > s_conf:
                        fields[key] = a_val
                        confidence[key] = a_conf
                    else:
                        fields[key] = s_val
                        confidence[key] = s_conf
                    source[key] = "conflict"
                    conflict_count += 1
                else:
                    # Values agree — structured wins if is_structured and high confidence
                    if raw.is_structured and s_conf >= _STRUCTURED_WIN_THRESHOLD:
                        fields[key] = s_val
                        confidence[key] = s_conf
                        source[key] = "structured_parser"
                    elif a_conf > s_conf:
                        fields[key] = a_val
                        confidence[key] = a_conf
                        source[key] = "ai_parser"
                    else:
                        fields[key] = s_val
                        confidence[key] = s_conf
                        source[key] = "structured_parser"
                    structured_count += 1

        # Build summary
        method_parts = []
        if raw.is_structured:
            method_parts.append("structured CSV")
        if ai_result:
            method_parts.append("AI")
        method = " + ".join(method_parts) if method_parts else "unknown"

        details = []
        if ai_filled_count:
            details.append(f"{ai_filled_count} field{'s' if ai_filled_count != 1 else ''} AI-filled")
        if conflict_count:
            details.append(f"{conflict_count} conflict{'s' if conflict_count != 1 else ''}")
        detail_str = f" ({', '.join(details)})" if details else ""

        summary = f"Parsed via {method}{detail_str}"

        return MergedParseResult(
            document_type=ai_result.document_type,
            fields=fields,
            confidence=confidence,
            source=source,
            conflicts=conflicts,
            parse_summary=summary,
        )

    @staticmethod
    def _flatten_structured(data: dict) -> dict:
        """Flatten structured parser output for field-level comparison."""
        flat: dict = {}
        for key, value in data.items():
            if isinstance(value, list):
                flat[key] = value
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        for sub_key, sub_val in item.items():
                            flat[f"{key}[{i}].{sub_key}"] = sub_val
            elif isinstance(value, dict):
                flat[key] = value
                for sub_key, sub_val in value.items():
                    flat[f"{key}.{sub_key}"] = sub_val
            else:
                flat[key] = value
        return flat

    @staticmethod
    def _values_conflict(a: Any, b: Any) -> bool:
        """Check if two values differ by more than 5% (for numeric) or differ at all (for non-numeric)."""
        if a is None or b is None:
            return False

        # Both numeric?
        try:
            fa = float(a)
            fb = float(b)
            if fa == 0.0 and fb == 0.0:
                return False
            denom = max(abs(fa), abs(fb))
            if denom == 0:
                return fa != fb
            return abs(fa - fb) / denom > _CONFLICT_THRESHOLD
        except (ValueError, TypeError):
            pass

        # Non-numeric: direct comparison
        return str(a).strip().lower() != str(b).strip().lower()
