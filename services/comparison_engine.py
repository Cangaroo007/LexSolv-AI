"""
Comparison Engine — SBR vs Liquidation comparison table (Annexure G format).

Calculates the dividend available to creditors under both a Small Business
Restructuring (SBR) plan and a hypothetical liquidation scenario, allowing
creditors to make an informed vote on the proposed plan.
"""

from __future__ import annotations


class ComparisonEngine:
    """
    Calculates SBR vs Liquidation comparison table (Annexure G format).
    """

    DEFAULT_RECOVERY_RATES: dict[str, float] = {
        "cash": 0.20,
        "receivables": 0.30,
        "inventory": 0.25,
        "loans_related": 0.30,
        "loans_shareholder": 0.00,
        "equipment": 0.25,
        "goodwill": 0.00,
    }

    def calculate(
        self,
        assets: list[dict],
        creditors_total: float,
        plan: dict,
    ) -> dict:
        """
        Calculate SBR vs Liquidation comparison.

        LIQUIDATION SCENARIO:
        1. For each asset: liquidation_value = book_value * recovery_pct
        2. Sum all liquidation values = total_recovered
        3. Deduct: est_liquidator_fees + est_legal_fees + est_disbursements
        4. available = max(0, total_recovered - total_fees)
        5. dividend_cents = round((available / creditors_total) * 100, 1)

        SBR SCENARIO:
        1. available = total_contribution - (total_contribution * fee_pct / 100)
        2. dividend_cents = round((available / creditors_total) * 100, 1)

        OUTPUT: dict matching ComparisonResult schema with:
        - lines: one per asset + fee lines + available + dividend
        - notes: numbered explanations for each line
        - summary figures

        Round dividend to 1 decimal place (47.1, not 47.11).

        Parameters
        ----------
        assets : list[dict]
            Each dict has: asset_type, description, book_value,
            liquidation_recovery_pct, liquidation_value.
        creditors_total : float
            Total concurrent creditor claims.
        plan : dict
            Keys matching PlanParameters: total_contribution,
            practitioner_fee_pct, est_liquidator_fees, est_legal_fees,
            est_disbursements.
        """
        # --- SBR Scenario ---
        total_contribution = plan["total_contribution"]
        fee_pct = plan.get("practitioner_fee_pct", 10.0)
        sbr_fees = total_contribution * fee_pct / 100.0
        sbr_available = total_contribution - sbr_fees

        if creditors_total > 0:
            sbr_dividend_cents = round((sbr_available / creditors_total) * 100, 1)
        else:
            sbr_dividend_cents = 0.0

        # --- Liquidation Scenario ---
        total_recovered = 0.0
        for asset in assets:
            total_recovered += asset["liquidation_value"]

        est_liquidator_fees = plan.get("est_liquidator_fees", 50000.0)
        est_legal_fees = plan.get("est_legal_fees", 10000.0)
        est_disbursements = plan.get("est_disbursements", 5000.0)
        total_liq_fees = est_liquidator_fees + est_legal_fees + est_disbursements

        liquidation_available = max(0.0, total_recovered - total_liq_fees)

        if creditors_total > 0:
            liquidation_dividend_cents = round(
                (liquidation_available / creditors_total) * 100, 1
            )
        else:
            liquidation_dividend_cents = 0.0

        # --- Build comparison lines ---
        lines: list[dict] = []
        notes: list[str] = []
        note_num = 1

        # Asset lines
        for asset in assets:
            notes.append(
                f"Note {note_num}: {asset['description']} — book value "
                f"${asset['book_value']:,.2f}, estimated recovery rate "
                f"{asset['liquidation_recovery_pct'] * 100:.0f}%."
            )
            lines.append({
                "description": asset["description"],
                "note_number": note_num,
                "sbr_value": None,
                "liquidation_value": asset["liquidation_value"],
            })
            note_num += 1

        # Total recovered line
        lines.append({
            "description": "Total estimated recovery",
            "note_number": None,
            "sbr_value": None,
            "liquidation_value": total_recovered,
        })

        # Fee lines — liquidation
        notes.append(
            f"Note {note_num}: Estimated liquidator fees based on "
            f"practitioner estimate."
        )
        lines.append({
            "description": "Less: Estimated liquidator fees",
            "note_number": note_num,
            "sbr_value": None,
            "liquidation_value": -est_liquidator_fees,
        })
        note_num += 1

        notes.append(
            f"Note {note_num}: Estimated legal fees for liquidation proceedings."
        )
        lines.append({
            "description": "Less: Estimated legal fees",
            "note_number": note_num,
            "sbr_value": None,
            "liquidation_value": -est_legal_fees,
        })
        note_num += 1

        notes.append(
            f"Note {note_num}: Estimated disbursements and incidental costs."
        )
        lines.append({
            "description": "Less: Estimated disbursements",
            "note_number": note_num,
            "sbr_value": None,
            "liquidation_value": -est_disbursements,
        })
        note_num += 1

        # SBR contribution line
        notes.append(
            f"Note {note_num}: Total contribution under the SBR plan proposal."
        )
        lines.append({
            "description": "Total contribution under SBR plan",
            "note_number": note_num,
            "sbr_value": total_contribution,
            "liquidation_value": None,
        })
        note_num += 1

        # SBR practitioner fees
        notes.append(
            f"Note {note_num}: Practitioner fees at "
            f"{fee_pct:.1f}% of total contribution."
        )
        lines.append({
            "description": "Less: Practitioner fees",
            "note_number": note_num,
            "sbr_value": -sbr_fees,
            "liquidation_value": None,
        })
        note_num += 1

        # Available for distribution
        lines.append({
            "description": "Available for distribution to creditors",
            "note_number": None,
            "sbr_value": sbr_available,
            "liquidation_value": liquidation_available,
        })

        # Dividend line
        lines.append({
            "description": "Estimated dividend (cents in the dollar)",
            "note_number": None,
            "sbr_value": sbr_dividend_cents,
            "liquidation_value": liquidation_dividend_cents,
        })

        # Total creditor claims note
        notes.append(
            f"Note {note_num}: Total concurrent creditor claims of "
            f"${creditors_total:,.2f} used for dividend calculation."
        )

        return {
            "lines": lines,
            "notes": notes,
            "sbr_available": sbr_available,
            "sbr_dividend_cents": sbr_dividend_cents,
            "liquidation_available": liquidation_available,
            "liquidation_dividend_cents": liquidation_dividend_cents,
            "total_creditor_claims": creditors_total,
        }

    def build_assets_from_balance_sheet(
        self,
        parsed_balance_sheet: dict,
    ) -> list[dict]:
        """
        Convert FileParser.parse_balance_sheet() output into asset entries
        with default recovery rates applied.  Practitioner overrides later.

        Mapping from parsed keys to asset types:
        - 'cash' -> 'cash'
        - 'receivables' -> 'receivables'
        - 'inventory' -> 'inventory'
        - 'loans_to_related' -> 'loans_related'
        - 'equipment' -> 'equipment'

        Note: 'total_liabilities' is excluded (not an asset).
        """
        key_to_asset_type = {
            "cash": ("cash", "Cash at Bank"),
            "receivables": ("receivables", "Accounts Receivable"),
            "inventory": ("inventory", "Inventory"),
            "loans_to_related": ("loans_related", "Loans to Related Entities"),
            "equipment": ("equipment", "Plant & Equipment"),
        }

        assets: list[dict] = []
        for parsed_key, (asset_type, description) in key_to_asset_type.items():
            book_value = parsed_balance_sheet.get(parsed_key, 0.0)
            if book_value == 0.0:
                continue
            recovery_pct = self.DEFAULT_RECOVERY_RATES.get(asset_type, 0.0)
            assets.append({
                "asset_type": asset_type,
                "description": description,
                "book_value": book_value,
                "liquidation_recovery_pct": recovery_pct,
                "liquidation_value": book_value * recovery_pct,
                "notes": "",
                "source": "parsed",
            })

        return assets
