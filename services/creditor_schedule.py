"""
Creditor Schedule Service — auto-classification, related-party flagging,
debt forgiveness, and voting eligibility for SBR proceedings.

Takes output from FileParser.parse_aged_payables() and builds a structured
creditor schedule with classification rules for Australian insolvency.
"""

from __future__ import annotations

import re


class CreditorScheduleService:
    """Build and manage creditor schedules for SBR proceedings."""

    # ------------------------------------------------------------------ #
    #  Classification patterns                                            #
    # ------------------------------------------------------------------ #

    _ATO_PATTERN = re.compile(r"australian taxation office|(?<!\w)ato(?!\w)", re.IGNORECASE)
    _ATO_ITA_PATTERN = re.compile(r"\bita\b", re.IGNORECASE)
    _ATO_ICA_PATTERN = re.compile(r"\b(?:ica|bas|gst)\b", re.IGNORECASE)

    _WORKERS_COMP_PATTERN = re.compile(r"icare|workcover|workers\s*comp", re.IGNORECASE)
    _FINANCE_PATTERN = re.compile(
        r"(?<!\w)(?:prospa|moula|ondeck|lumi|zip\s*business)(?!\w)", re.IGNORECASE
    )

    # ------------------------------------------------------------------ #
    #  Build from parsed data                                             #
    # ------------------------------------------------------------------ #

    def build_from_parsed(self, parsed_creditors: list[dict]) -> list[dict]:
        """
        Takes output from FileParser.parse_aged_payables() and builds
        a structured creditor schedule with auto-classification.

        Classification rules:
        - Name contains 'Australian Taxation Office' or 'ATO':
          - 'ITA' in name → category='ato_ita'
          - 'ICA'/'BAS'/'GST' in name → category='ato_ica'
          - else → category='ato_other'
        - Name contains 'iCare'/'WorkCover'/'workers comp' → category='workers_comp'
        - Name contains 'Prospa'/'Moula'/'OnDeck'/'Lumi'/'Zip Business' → category='finance'
        - Default → category='trade'

        All creditors start with:
        - is_related_party=False, can_vote=True, status='admitted', source='parsed'
        """
        results: list[dict] = []
        for entry in parsed_creditors:
            creditor = {
                "creditor_name": entry["creditor_name"],
                "amount_claimed": entry["amount_claimed"],
                "category": self._classify(entry["creditor_name"]),
                "is_related_party": False,
                "is_secured": False,
                "can_vote": True,
                "status": "admitted",
                "source": "parsed",
                "notes": None,
            }
            results.append(creditor)
        return results

    # ------------------------------------------------------------------ #
    #  Related-party flagging                                             #
    # ------------------------------------------------------------------ #

    def flag_related_party(self, creditor: dict, is_related: bool) -> dict:
        """Toggle related-party status. If related, set can_vote=False."""
        creditor["is_related_party"] = is_related
        creditor["can_vote"] = not is_related
        return creditor

    # ------------------------------------------------------------------ #
    #  Status updates                                                     #
    # ------------------------------------------------------------------ #

    def update_status(self, creditor: dict, new_status: str) -> dict:
        """Update status: admitted, disputed, forgiven, excluded."""
        creditor["status"] = new_status
        return creditor

    # ------------------------------------------------------------------ #
    #  Totals calculation                                                 #
    # ------------------------------------------------------------------ #

    def calculate_totals(self, creditors: list[dict]) -> dict:
        """
        Returns:
        - total_claims: sum of all amount_claimed
        - total_admitted: sum where status='admitted'
        - total_voting: sum where can_vote=True AND status='admitted'
        - total_excluded: count where status in ('forgiven', 'excluded')
        - related_party_total: sum where is_related_party=True

        Forgiven debts EXCLUDED from admitted totals.
        Disputed debts INCLUDED in totals but flagged.
        Related parties INCLUDED in distribution but EXCLUDED from voting.
        """
        total_claims = 0.0
        total_admitted = 0.0
        total_voting = 0.0
        total_excluded = 0
        related_party_total = 0.0

        for c in creditors:
            amount = c["amount_claimed"]
            status = c["status"]

            total_claims += amount

            if status == "admitted":
                total_admitted += amount
                if c.get("can_vote", True):
                    total_voting += amount

            if status in ("forgiven", "excluded"):
                total_excluded += 1

            if c.get("is_related_party", False):
                related_party_total += amount

        return {
            "total_claims": total_claims,
            "total_admitted": total_admitted,
            "total_voting": total_voting,
            "total_excluded": total_excluded,
            "related_party_total": related_party_total,
        }

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _classify(self, name: str) -> str:
        """Classify a creditor name into a category."""
        if self._ATO_PATTERN.search(name):
            if self._ATO_ITA_PATTERN.search(name):
                return "ato_ita"
            if self._ATO_ICA_PATTERN.search(name):
                return "ato_ica"
            return "ato_other"

        if self._WORKERS_COMP_PATTERN.search(name):
            return "workers_comp"

        if self._FINANCE_PATTERN.search(name):
            return "finance"

        return "trade"
