"""
Payment Schedule Generator — instalment schedule for SBR plan contributions.

Generates a detailed payment schedule showing initial and ongoing payments
with practitioner fee breakdowns for each instalment.
"""

from __future__ import annotations


class PaymentScheduleGenerator:
    """Generate instalment schedules from SBR plan parameters."""

    def generate(self, plan: dict) -> dict:
        """
        Generate instalment schedule from plan parameters.

        - First num_initial_payments at initial_payment_amount
        - Remaining num_ongoing_payments at ongoing_payment_amount
        - Each payment: fee = total_payment * (fee_pct / 100), net = total - fee
        - Month labels: 'Month 1', 'Month 2', etc.

        VALIDATION: sum of all total_payments must equal total_contribution.
        If not, raise ValueError with the discrepancy amount.

        Parameters
        ----------
        plan : dict
            Keys matching PlanParameters: total_contribution,
            practitioner_fee_pct, num_initial_payments,
            initial_payment_amount, num_ongoing_payments,
            ongoing_payment_amount.

        Returns
        -------
        dict
            Matching PaymentScheduleResult schema with entries,
            total_contribution, total_fees, total_net_dividend.
        """
        total_contribution = plan["total_contribution"]
        fee_pct = plan.get("practitioner_fee_pct", 10.0)
        num_initial = plan.get("num_initial_payments", 2)
        initial_amount = plan.get("initial_payment_amount", 0.0)
        num_ongoing = plan.get("num_ongoing_payments", 22)
        ongoing_amount = plan.get("ongoing_payment_amount", 0.0)

        # Validate that payments sum to total_contribution
        calculated_total = (num_initial * initial_amount) + (num_ongoing * ongoing_amount)
        if abs(calculated_total - total_contribution) > 0.01:
            discrepancy = calculated_total - total_contribution
            raise ValueError(
                f"Payment schedule does not balance. "
                f"Sum of payments (${calculated_total:,.2f}) differs from "
                f"total contribution (${total_contribution:,.2f}) "
                f"by ${discrepancy:,.2f}."
            )

        entries: list[dict] = []
        total_fees = 0.0
        total_net = 0.0
        payment_number = 1

        # Initial payments
        for i in range(num_initial):
            fee = initial_amount * fee_pct / 100.0
            net = initial_amount - fee
            entries.append({
                "payment_number": payment_number,
                "month_label": f"Month {payment_number}",
                "due_date": None,
                "net_dividend": net,
                "practitioner_fee": fee,
                "total_payment": initial_amount,
            })
            total_fees += fee
            total_net += net
            payment_number += 1

        # Ongoing payments
        for i in range(num_ongoing):
            fee = ongoing_amount * fee_pct / 100.0
            net = ongoing_amount - fee
            entries.append({
                "payment_number": payment_number,
                "month_label": f"Month {payment_number}",
                "due_date": None,
                "net_dividend": net,
                "practitioner_fee": fee,
                "total_payment": ongoing_amount,
            })
            total_fees += fee
            total_net += net
            payment_number += 1

        return {
            "entries": entries,
            "total_contribution": total_contribution,
            "total_fees": total_fees,
            "total_net_dividend": total_net,
        }
