"""
LexSolv AI — Forensic Analysis Engine.

This is the core value of the platform: it replaces offshore analysts by
automatically detecting preference payments, related-party transactions,
and calculating solvency scores from accounting data.

All methods accept normalised `Transaction` objects (from Xero/MYOB) and
return structured Pydantic models ready for frontend dashboard display.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from decimal import Decimal
from typing import Optional

from models.schemas import (
    ForensicReport,
    PreferencePaymentFlag,
    PreferencePaymentReport,
    RelatedPartyFlag,
    RelatedPartyReport,
    RiskLevel,
    SolvencyScore,
    Transaction,
    TransactionType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds — tuneable per engagement; these are sensible defaults
# ---------------------------------------------------------------------------

# Payments above this amount (AUD) get extra scrutiny
_LARGE_PAYMENT_THRESHOLD = Decimal("10000.00")

# Very large payments get critical risk
_CRITICAL_PAYMENT_THRESHOLD = Decimal("50000.00")

# Account codes commonly associated with non-essential / discretionary spend
_NON_ESSENTIAL_ACCOUNT_PREFIXES = (
    "4",      # Expenses
    "6",      # Other expenses
    "8",      # Drawings / distributions
    "9",      # Extraordinary items
)

# Transaction types that count as "payments out"
_PAYMENT_TYPES = {
    TransactionType.PAYMENT,
    TransactionType.BANK_TRANSACTION,
}


class ForensicAnalyzer:
    """
    Analyses normalised accounting transactions for insolvency red flags.

    Usage
    -----
    >>> analyzer = ForensicAnalyzer()
    >>> report = analyzer.full_report(
    ...     transactions=txns,
    ...     insolvency_date=date(2026, 2, 1),
    ...     director_names=["John Smith", "Jane Doe"],
    ...     current_assets=Decimal("500000"),
    ...     current_liabilities=Decimal("800000"),
    ...     company_name="Acme Pty Ltd",
    ... )
    """

    # ------------------------------------------------------------------
    # 1. Preference Payment Detection
    # ------------------------------------------------------------------

    def detect_preference_payments(
        self,
        transactions: list[Transaction],
        insolvency_date: date,
        threshold_days: int = 90,
    ) -> PreferencePaymentReport:
        """
        Flag large payments made to non-essential creditors shortly before
        the insolvency date.

        Under the Corporations Act 2001 (Cth) s 588FA, a transaction is an
        "unfair preference" if it was made within 6 months of the relation-back
        day and the creditor received more than they would in a winding up.

        This heuristic flags payments:
          - Made within `threshold_days` before `insolvency_date`
          - Of type PAYMENT or BANK_TRANSACTION
          - Above the large-payment threshold
          - Optionally to non-essential account codes

        Parameters
        ----------
        transactions : list[Transaction]
            Normalised transaction records from accounting system.
        insolvency_date : date
            The date of appointment / relation-back day.
        threshold_days : int
            Look-back window in days (default 90; set to 180 for full s 588FA).

        Returns
        -------
        PreferencePaymentReport
            Structured report with flagged transactions and summary.
        """
        flags: list[PreferencePaymentFlag] = []

        for txn in transactions:
            # Only analyse outbound payments
            if txn.transaction_type not in _PAYMENT_TYPES:
                continue

            # Calculate days before insolvency
            days_before = (insolvency_date - txn.date).days
            if days_before < 0 or days_before > threshold_days:
                continue

            # Skip small payments
            if txn.amount < _LARGE_PAYMENT_THRESHOLD:
                continue

            # Determine risk level based on amount and timing
            risk_level = self._preference_risk(txn.amount, days_before)

            # Build the human-readable reason
            reasons = []
            reasons.append(
                f"${txn.amount:,.2f} payment made {days_before} days before insolvency"
            )

            if txn.amount >= _CRITICAL_PAYMENT_THRESHOLD:
                reasons.append(f"exceeds critical threshold (${_CRITICAL_PAYMENT_THRESHOLD:,.2f})")

            if self._is_non_essential_account(txn.account_code):
                reasons.append(f"non-essential account code ({txn.account_code})")
                # Bump risk if it's also to a non-essential account
                if risk_level == RiskLevel.MEDIUM:
                    risk_level = RiskLevel.HIGH

            if days_before <= 30:
                reasons.append("within 30-day critical window")

            flags.append(
                PreferencePaymentFlag(
                    transaction_id=txn.id,
                    reference=txn.reference,
                    date=txn.date,
                    amount=txn.amount,
                    contact_name=txn.contact_name,
                    description=txn.description,
                    days_before_insolvency=days_before,
                    risk_level=risk_level,
                    reason="; ".join(reasons),
                )
            )

        # Sort by risk (critical first), then by amount descending
        risk_order = {RiskLevel.CRITICAL: 0, RiskLevel.HIGH: 1, RiskLevel.MEDIUM: 2, RiskLevel.LOW: 3}
        flags.sort(key=lambda f: (risk_order.get(f.risk_level, 9), -f.amount))

        total_amount = sum(f.amount for f in flags)
        critical_count = sum(1 for f in flags if f.risk_level == RiskLevel.CRITICAL)

        if not flags:
            summary = f"No preference payments detected in the {threshold_days}-day look-back window."
        else:
            summary = (
                f"{len(flags)} potential preference payment(s) totalling "
                f"${total_amount:,.2f} detected within {threshold_days} days of insolvency. "
                f"{critical_count} flagged as critical."
            )

        logger.info("Preference payment scan: %d flagged, $%s total", len(flags), total_amount)

        return PreferencePaymentReport(
            insolvency_date=insolvency_date,
            threshold_days=threshold_days,
            total_flagged=len(flags),
            total_flagged_amount=total_amount,
            flags=flags,
            summary=summary,
        )

    # ------------------------------------------------------------------
    # 2. Related-Party Transaction Detection
    # ------------------------------------------------------------------

    def identify_related_parties(
        self,
        transactions: list[Transaction],
        director_names_list: list[str],
    ) -> RelatedPartyReport:
        """
        Flag any transactions where the payee/contact or description matches
        names from the director/related-party list.

        Uses fuzzy substring matching to catch variations like:
          - "J Smith" matching "John Smith"
          - "Smith Holdings" matching director "Smith"
          - Description containing "Payment to John Smith for consulting"

        Parameters
        ----------
        transactions : list[Transaction]
            Normalised transaction records.
        director_names_list : list[str]
            Names of directors, officeholders, and known related parties.

        Returns
        -------
        RelatedPartyReport
            Structured report with flagged transactions.
        """
        if not director_names_list:
            return RelatedPartyReport(
                director_names=[],
                total_flagged=0,
                total_flagged_amount=Decimal("0.00"),
                flags=[],
                summary="No director names provided — related-party scan skipped.",
            )

        # Build match patterns: full name, surname, and initials
        patterns = self._build_name_patterns(director_names_list)
        flags: list[RelatedPartyFlag] = []

        for txn in transactions:
            # Check contact_name
            if txn.contact_name:
                matched = self._match_against_patterns(txn.contact_name, patterns)
                if matched:
                    flags.append(self._build_related_party_flag(
                        txn, matched_director=matched, match_field="contact_name",
                    ))
                    continue  # Don't double-flag the same transaction

            # Check description
            if txn.description:
                matched = self._match_against_patterns(txn.description, patterns)
                if matched:
                    flags.append(self._build_related_party_flag(
                        txn, matched_director=matched, match_field="description",
                    ))

        # Sort by amount descending
        flags.sort(key=lambda f: -f.amount)

        total_amount = sum(f.amount for f in flags)

        if not flags:
            summary = (
                f"No related-party transactions detected for "
                f"{len(director_names_list)} director name(s)."
            )
        else:
            summary = (
                f"{len(flags)} related-party transaction(s) totalling "
                f"${total_amount:,.2f} detected across "
                f"{len(set(f.matched_director for f in flags))} director(s)."
            )

        logger.info("Related-party scan: %d flagged, $%s total", len(flags), total_amount)

        return RelatedPartyReport(
            director_names=director_names_list,
            total_flagged=len(flags),
            total_flagged_amount=total_amount,
            flags=flags,
            summary=summary,
        )

    # ------------------------------------------------------------------
    # 3. Solvency Score / Liquidation vs SBR Ratio
    # ------------------------------------------------------------------

    def calculate_solvency_score(
        self,
        current_assets: Decimal,
        current_liabilities: Decimal,
    ) -> SolvencyScore:
        """
        Calculate a quick solvency assessment based on current assets vs
        current liabilities.

        The score drives the Liquidation vs SBR (Small Business Restructuring)
        recommendation:

        - **Ratio >= 1.0** → Technically solvent — may not need formal process
        - **Ratio 0.5–1.0** → SBR candidate — viable for restructuring
        - **Ratio 0.25–0.5** → Borderline — SBR possible but risky
        - **Ratio < 0.25** → Liquidation likely — insufficient assets to restructure

        The 0-100 score maps the ratio to a dashboard-friendly gauge.

        Parameters
        ----------
        current_assets : Decimal
            Total current assets from the balance sheet.
        current_liabilities : Decimal
            Total current liabilities from the balance sheet.

        Returns
        -------
        SolvencyScore
            Structured solvency assessment.
        """
        net_position = current_assets - current_liabilities

        if current_liabilities > 0:
            ratio = float(current_assets / current_liabilities)
        else:
            ratio = float("inf") if current_assets > 0 else 1.0

        # Map ratio to 0-100 score
        # ratio 0.0 → score ~5, ratio 0.5 → score ~35, ratio 1.0 → score ~65, ratio 1.5+ → score ~90+
        if ratio >= 2.0:
            score = 100
        elif ratio >= 1.0:
            score = int(65 + (ratio - 1.0) * 35)  # 65-100
        elif ratio >= 0.5:
            score = int(35 + (ratio - 0.5) * 60)  # 35-65
        elif ratio >= 0.25:
            score = int(15 + (ratio - 0.25) * 80)  # 15-35
        else:
            score = max(0, int(ratio * 60))  # 0-15

        score = max(0, min(100, score))

        # Determine recommendation and risk
        if ratio >= 1.0:
            recommendation = "solvent"
            risk_level = RiskLevel.LOW
            explanation = (
                f"Current ratio of {ratio:.2f} indicates the company can meet "
                f"its short-term obligations. Net position: ${net_position:,.2f}. "
                f"Formal insolvency process may not be required."
            )
        elif ratio >= 0.5:
            recommendation = "sbr_candidate"
            risk_level = RiskLevel.MEDIUM
            explanation = (
                f"Current ratio of {ratio:.2f} suggests the company is insolvent "
                f"but has sufficient assets for a Small Business Restructuring (SBR). "
                f"Net deficit: ${abs(net_position):,.2f}. "
                f"Restructuring plan could achieve better outcome than liquidation."
            )
        elif ratio >= 0.25:
            recommendation = "sbr_candidate"
            risk_level = RiskLevel.HIGH
            explanation = (
                f"Current ratio of {ratio:.2f} is borderline. "
                f"Net deficit: ${abs(net_position):,.2f}. "
                f"SBR may be possible but creditor returns would be marginal. "
                f"Detailed viability analysis recommended before proceeding."
            )
        else:
            recommendation = "liquidation"
            risk_level = RiskLevel.CRITICAL
            explanation = (
                f"Current ratio of {ratio:.2f} indicates severe insolvency. "
                f"Net deficit: ${abs(net_position):,.2f}. "
                f"Insufficient assets to support restructuring — "
                f"liquidation is the recommended pathway."
            )

        logger.info(
            "Solvency score: %d/100 (ratio=%.2f, recommendation=%s)",
            score, ratio, recommendation,
        )

        return SolvencyScore(
            current_assets=current_assets,
            current_liabilities=current_liabilities,
            net_position=net_position,
            solvency_ratio=round(ratio, 4),
            score=score,
            recommendation=recommendation,
            risk_level=risk_level,
            explanation=explanation,
        )

    # ------------------------------------------------------------------
    # 4. Combined Full Report
    # ------------------------------------------------------------------

    def full_report(
        self,
        transactions: list[Transaction],
        insolvency_date: date,
        director_names: list[str],
        current_assets: Decimal,
        current_liabilities: Decimal,
        company_name: Optional[str] = None,
        threshold_days: int = 90,
    ) -> ForensicReport:
        """
        Run all forensic checks and return a single combined report.

        This is the primary entry point for the forensic engine — call it
        after pulling data from Xero/MYOB to get a dashboard-ready payload.

        Parameters
        ----------
        transactions : list[Transaction]
            All normalised transactions for the company.
        insolvency_date : date
            Appointment / relation-back day.
        director_names : list[str]
            Director and officeholder names for related-party checks.
        current_assets : Decimal
            Total current assets.
        current_liabilities : Decimal
            Total current liabilities.
        company_name : str, optional
            For display in the report header.
        threshold_days : int
            Look-back window for preference payments (default 90).

        Returns
        -------
        ForensicReport
            The complete forensic analysis, ready for JSON serialization.
        """
        pref_report = self.detect_preference_payments(
            transactions, insolvency_date, threshold_days,
        )
        rp_report = self.identify_related_parties(
            transactions, director_names,
        )
        solvency = self.calculate_solvency_score(
            current_assets, current_liabilities,
        )

        # Determine overall risk (worst of the three)
        risk_priority = {
            RiskLevel.CRITICAL: 0,
            RiskLevel.HIGH: 1,
            RiskLevel.MEDIUM: 2,
            RiskLevel.LOW: 3,
            RiskLevel.INFO: 4,
        }
        all_risks = [solvency.risk_level]
        if pref_report.flags:
            all_risks.append(pref_report.flags[0].risk_level)
        if rp_report.flags:
            all_risks.append(rp_report.flags[0].risk_level)

        overall_risk = min(all_risks, key=lambda r: risk_priority.get(r, 9))

        alert_count = pref_report.total_flagged + rp_report.total_flagged
        if solvency.recommendation == "liquidation":
            alert_count += 1

        # Build top-level summary
        parts = []
        if pref_report.total_flagged:
            parts.append(f"{pref_report.total_flagged} preference payment(s)")
        if rp_report.total_flagged:
            parts.append(f"{rp_report.total_flagged} related-party transaction(s)")
        parts.append(f"solvency score {solvency.score}/100 ({solvency.recommendation})")

        summary = (
            f"Forensic analysis complete for {company_name or 'company'}: "
            + ", ".join(parts)
            + f". Overall risk: {overall_risk.value}."
        )

        return ForensicReport(
            company_name=company_name,
            analysis_date=date.today(),
            preference_payments=pref_report,
            related_parties=rp_report,
            solvency=solvency,
            overall_risk=overall_risk,
            alert_count=alert_count,
            summary=summary,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _preference_risk(amount: Decimal, days_before: int) -> RiskLevel:
        """Assign a risk level based on payment amount and proximity to insolvency."""
        if amount >= _CRITICAL_PAYMENT_THRESHOLD and days_before <= 30:
            return RiskLevel.CRITICAL
        if amount >= _CRITICAL_PAYMENT_THRESHOLD or days_before <= 30:
            return RiskLevel.HIGH
        if amount >= _LARGE_PAYMENT_THRESHOLD:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    @staticmethod
    def _is_non_essential_account(account_code: Optional[str]) -> bool:
        """Check if the account code belongs to a non-essential expense category."""
        if not account_code:
            return False
        return account_code.startswith(_NON_ESSENTIAL_ACCOUNT_PREFIXES)

    @staticmethod
    def _build_name_patterns(names: list[str]) -> list[tuple[str, re.Pattern]]:
        """
        Build regex patterns from director names.

        For "John Smith" we generate patterns matching:
          - "john smith" (full name, case-insensitive)
          - "smith" (surname)
          - "j smith" or "j. smith" (initial + surname)
        """
        patterns: list[tuple[str, re.Pattern]] = []

        for name in names:
            name_clean = name.strip()
            if not name_clean:
                continue

            # Full name match (word-boundary)
            patterns.append((
                name_clean,
                re.compile(re.escape(name_clean), re.IGNORECASE),
            ))

            parts = name_clean.split()
            if len(parts) >= 2:
                surname = parts[-1]
                first_initial = parts[0][0]

                # Surname only (word-boundary to avoid partial matches on short names)
                if len(surname) >= 4:
                    patterns.append((
                        name_clean,
                        re.compile(r"\b" + re.escape(surname) + r"\b", re.IGNORECASE),
                    ))

                # Initial + surname: "J Smith", "J. Smith"
                patterns.append((
                    name_clean,
                    re.compile(
                        r"\b" + re.escape(first_initial) + r"\.?\s*" + re.escape(surname) + r"\b",
                        re.IGNORECASE,
                    ),
                ))

        return patterns

    @staticmethod
    def _match_against_patterns(
        text: str,
        patterns: list[tuple[str, re.Pattern]],
    ) -> Optional[str]:
        """Return the matched director name if any pattern matches, else None."""
        for director_name, pattern in patterns:
            if pattern.search(text):
                return director_name
        return None

    @staticmethod
    def _build_related_party_flag(
        txn: Transaction,
        matched_director: str,
        match_field: str,
    ) -> RelatedPartyFlag:
        """Build a RelatedPartyFlag from a matched transaction."""
        # Risk based on amount
        if txn.amount >= _CRITICAL_PAYMENT_THRESHOLD:
            risk_level = RiskLevel.CRITICAL
        elif txn.amount >= _LARGE_PAYMENT_THRESHOLD:
            risk_level = RiskLevel.HIGH
        else:
            risk_level = RiskLevel.MEDIUM

        field_value = txn.contact_name if match_field == "contact_name" else txn.description
        reason = (
            f"Transaction {match_field} '{field_value}' matches director/related-party "
            f"'{matched_director}' — ${txn.amount:,.2f} on {txn.date.isoformat()}"
        )

        return RelatedPartyFlag(
            transaction_id=txn.id,
            reference=txn.reference,
            date=txn.date,
            amount=txn.amount,
            contact_name=txn.contact_name,
            description=txn.description,
            matched_director=matched_director,
            match_field=match_field,
            risk_level=risk_level,
            reason=reason,
        )
