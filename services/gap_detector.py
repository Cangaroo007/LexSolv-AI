"""
LexSolv AI — Gap Detection Engine.

Compares extracted document data against the fields required to run
the comparison engine.  Categorises gaps by severity.

Gap severity:
- BLOCKING: comparison engine cannot run without this field
- ADVISORY: comparison will run but result quality is reduced
- LOW_CONFIDENCE: field present but confidence < 0.6, flagged for practitioner review
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from models.schemas import DirectorQuestion, GapItem, GapReport, PractitionerItem
from services.parser_merger import MergedParseResult

logger = logging.getLogger(__name__)

_LOW_CONFIDENCE_THRESHOLD = 0.6


class GapDetector:
    """
    Compares extracted document data against the fields required to run
    the comparison engine.  Categorises gaps by severity.

    Gap severity:
    - BLOCKING: comparison engine cannot run without this field
    - ADVISORY: comparison will run but result quality is reduced
    - LOW_CONFIDENCE: field present but confidence < 0.6, flagged for practitioner review
    """

    # What the comparison engine needs — mirrors comparison_engine.py requirements exactly
    COMPARISON_ENGINE_REQUIREMENTS: dict[str, dict[str, list[str]]] = {
        "aged_payables": {
            "blocking": ["creditors", "total_claims"],
            "advisory": ["creditor[*].category", "creditor[*].is_related_party"],
        },
        "balance_sheet": {
            "blocking": ["total_liabilities"],
            "advisory": ["assets", "asset[*].recovery_pct"],
        },
        "bank_statement": {
            "blocking": ["closing_balance"],
            "advisory": ["period_end_date"],
        },
        "plan_parameters": {
            "blocking": ["total_contribution", "practitioner_fee_pct"],
            "advisory": ["num_initial_payments", "initial_payment_amount"],
        },
    }

    # Plain-English questions per missing field — two versions: practitioner and director
    GAP_QUESTIONS: dict[str, dict[str, str | None]] = {
        "closing_balance": {
            "practitioner": "Closing bank balance not found. Enter manually or upload a clearer bank statement.",
            "director": "What is the current balance of the company's main bank account?",
        },
        "total_liabilities": {
            "practitioner": "Total liabilities not extracted from balance sheet. Enter manually or re-upload.",
            "director": "What is the total amount the company owes to all creditors as of the appointment date?",
        },
        "creditor[*].category": {
            "practitioner": "Some creditors could not be auto-classified. Review the creditor schedule.",
            "director": "For each creditor listed, are any related parties — family members, associates, or entities you control?",
        },
        "total_contribution": {
            "practitioner": "SBR plan contribution amount not set. Required to calculate the SBR dividend.",
            "director": "How much is the company (or a related party) proposing to contribute under the restructuring plan?",
        },
        "practitioner_fee_pct": {
            "practitioner": "Practitioner fee percentage not set. Enter in Plan Parameters.",
            "director": None,  # Not a director-facing question
        },
        "asset[*].recovery_pct": {
            "practitioner": "Recovery percentages not set for one or more assets. Review the asset register.",
            "director": "For each asset listed, do you have a recent valuation or sale estimate?",
        },
        "period_end_date": {
            "practitioner": "Bank statement date not found — comparison may use incorrect period.",
            "director": "What date does the bank statement cover up to?",
        },
        "creditors": {
            "practitioner": "Creditor schedule not parsed. Upload the aged payables file.",
            "director": "Can you provide a list of all the company's creditors and what is owed to each?",
        },
        "total_claims": {
            "practitioner": "Total creditor claims not calculated. Upload or verify the aged payables file.",
            "director": "What is the total amount owed to all creditors?",
        },
        "assets": {
            "practitioner": "Asset register not found in the balance sheet. Upload or enter manually.",
            "director": "Can you provide a list of the company's assets and their approximate values?",
        },
        "creditor[*].is_related_party": {
            "practitioner": "Related party flags not set for creditors. Review the creditor schedule.",
            "director": "Are any of the listed creditors related parties — family, associates, or entities you control?",
        },
        "num_initial_payments": {
            "practitioner": "Number of initial payments not set. Configure in Plan Parameters.",
            "director": None,
        },
        "initial_payment_amount": {
            "practitioner": "Initial payment amount not set. Configure in Plan Parameters.",
            "director": None,
        },
    }

    # Topic classification for director questionnaire grouping
    _FIELD_TOPICS: dict[str, str] = {
        "closing_balance": "financial",
        "total_liabilities": "financial",
        "total_contribution": "financial",
        "total_claims": "financial",
        "assets": "financial",
        "creditors": "creditors",
        "creditor[*].category": "creditors",
        "creditor[*].is_related_party": "creditors",
        "period_end_date": "operations",
        "asset[*].recovery_pct": "financial",
        "practitioner_fee_pct": "operations",
        "num_initial_payments": "operations",
        "initial_payment_amount": "financial",
    }

    # Topic sort order for director questionnaire
    _TOPIC_ORDER = {"financial": 0, "creditors": 1, "operations": 2}

    def detect(
        self,
        engagement_id: str,
        uploaded_documents: dict[str, MergedParseResult | None],
        plan_parameters: dict | None,
    ) -> GapReport:
        """
        Run gap detection against uploaded documents and plan parameters.

        uploaded_documents keys: "aged_payables" | "balance_sheet" | "bank_statement" | "pnl"
        Value is None if that document type has not been uploaded yet.
        """
        blocking_gaps: list[GapItem] = []
        advisory_gaps: list[GapItem] = []
        low_confidence_fields: list[GapItem] = []
        missing_documents: list[str] = []

        total_required = 0
        present_count = 0

        for doc_type, requirements in self.COMPARISON_ENGINE_REQUIREMENTS.items():
            # Check if the document has been uploaded at all
            if doc_type == "plan_parameters":
                # Plan parameters come from a separate dict, not from document uploads
                doc_result = None
                doc_fields = plan_parameters or {}
                doc_confidence: dict[str, float] = {}
                # Plan parameters entered by practitioner have confidence 1.0
                for k in doc_fields:
                    if doc_fields[k] is not None:
                        doc_confidence[k] = 1.0
                if not plan_parameters:
                    missing_documents.append("plan_parameters")
            else:
                doc_result = uploaded_documents.get(doc_type)
                if doc_result is None:
                    missing_documents.append(doc_type)
                    doc_fields = {}
                    doc_confidence = {}
                else:
                    doc_fields = doc_result.fields
                    doc_confidence = doc_result.confidence

            # Check blocking fields
            for field in requirements.get("blocking", []):
                total_required += 1
                value = doc_fields.get(field)
                confidence = doc_confidence.get(field, 0.0)

                if value is None or (isinstance(value, (list, dict)) and not value):
                    # Field missing → blocking gap
                    gap = self._make_gap_item(field, doc_type, "blocking", None, 0.0)
                    blocking_gaps.append(gap)
                elif confidence < _LOW_CONFIDENCE_THRESHOLD:
                    # Present but low confidence → both low_confidence and track as present
                    gap = self._make_gap_item(
                        field, doc_type, "low_confidence", value, confidence
                    )
                    low_confidence_fields.append(gap)
                    present_count += 1
                else:
                    present_count += 1

            # Check advisory fields
            for field in requirements.get("advisory", []):
                total_required += 1
                value = doc_fields.get(field)
                confidence = doc_confidence.get(field, 0.0)

                if value is None or (isinstance(value, (list, dict)) and not value):
                    gap = self._make_gap_item(field, doc_type, "advisory", None, 0.0)
                    advisory_gaps.append(gap)
                elif confidence < _LOW_CONFIDENCE_THRESHOLD:
                    gap = self._make_gap_item(
                        field, doc_type, "low_confidence", value, confidence
                    )
                    low_confidence_fields.append(gap)
                    present_count += 1
                else:
                    present_count += 1

        # Completion percentage
        completion_pct = round(
            (present_count / total_required) * 100, 1
        ) if total_required > 0 else 0.0

        return GapReport(
            engagement_id=engagement_id,
            generated_at=datetime.now(timezone.utc),
            blocking_gaps=blocking_gaps,
            advisory_gaps=advisory_gaps,
            low_confidence_fields=low_confidence_fields,
            can_run_comparison=len(blocking_gaps) == 0,
            completion_pct=completion_pct,
            missing_documents=missing_documents,
        )

    def can_run_comparison(self, gap_report: GapReport) -> bool:
        """True only if blocking_gaps is empty."""
        return len(gap_report.blocking_gaps) == 0

    def get_director_questionnaire(
        self, gap_report: GapReport
    ) -> list[DirectorQuestion]:
        """
        Returns ordered list of gap questions suitable for the director.
        Excludes practitioner-only gaps (director=None in GAP_QUESTIONS).
        Groups by topic: financial → creditors → operations.
        """
        all_gaps = (
            gap_report.blocking_gaps
            + gap_report.advisory_gaps
            + gap_report.low_confidence_fields
        )

        questions: list[DirectorQuestion] = []
        for gap in all_gaps:
            q_info = self.GAP_QUESTIONS.get(gap.field, {})
            director_q = q_info.get("director") if isinstance(q_info, dict) else None
            if director_q is None:
                continue

            topic = self._FIELD_TOPICS.get(gap.field, "operations")
            questions.append(
                DirectorQuestion(
                    order=0,  # Will be re-numbered after sorting
                    topic=topic,
                    field=gap.field,
                    question=director_q,
                )
            )

        # Sort by topic order, then by field name for consistency
        questions.sort(
            key=lambda q: (self._TOPIC_ORDER.get(q.topic, 99), q.field)
        )

        # Re-number
        for i, q in enumerate(questions, start=1):
            q.order = i

        return questions

    def get_practitioner_checklist(
        self, gap_report: GapReport
    ) -> list[PractitionerItem]:
        """Returns all gaps as an ordered checklist for the practitioner."""
        severity_order = {"blocking": 0, "advisory": 1, "low_confidence": 2}

        all_gaps = (
            gap_report.blocking_gaps
            + gap_report.advisory_gaps
            + gap_report.low_confidence_fields
        )

        items: list[PractitionerItem] = []
        for gap in all_gaps:
            items.append(
                PractitionerItem(
                    order=0,
                    severity=gap.severity,
                    field=gap.field,
                    document_type=gap.document_type,
                    instruction=gap.practitioner_prompt,
                )
            )

        # Sort by severity (blocking first), then by document type, then field
        items.sort(
            key=lambda it: (
                severity_order.get(it.severity, 99),
                it.document_type,
                it.field,
            )
        )

        # Re-number
        for i, item in enumerate(items, start=1):
            item.order = i

        return items

    def _make_gap_item(
        self,
        field: str,
        document_type: str,
        severity: str,
        current_value: Any | None,
        current_confidence: float,
    ) -> GapItem:
        """Build a GapItem with questions from the GAP_QUESTIONS map."""
        q_info = self.GAP_QUESTIONS.get(field, {})
        practitioner_prompt = (
            q_info.get("practitioner", f"Field '{field}' is missing from {document_type}.")
            if isinstance(q_info, dict)
            else f"Field '{field}' is missing from {document_type}."
        )
        director_question = (
            q_info.get("director")
            if isinstance(q_info, dict)
            else None
        )

        # Determine if the field can be auto-filled from other data
        # For now, simple heuristic: total_claims can be derived from creditors
        can_autofill = field in ("total_claims",)

        return GapItem(
            field=field,
            document_type=document_type,
            severity=severity,
            current_value=current_value,
            current_confidence=current_confidence,
            practitioner_prompt=practitioner_prompt,
            director_question=director_question,
            can_autofill=can_autofill,
        )
