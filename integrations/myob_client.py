"""
LexSolv AI — MYOB AccountRight / Essentials integration client.

MYOB uses a similar OAuth2 flow but with its own endpoints and SDK.
Reference: https://developer.myob.com/api/accountright/v2/

This module contains placeholder methods that mirror the MYOB API structure.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

from models.schemas import (
    CompanyData,
    CreditorEntry,
    CreditorList,
    Transaction,
    TransactionType,
    TransactionStatus,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OAuth2 token container
# ---------------------------------------------------------------------------

@dataclass
class MYOBTokenSet:
    """Lightweight container for MYOB OAuth2 tokens."""

    access_token: str = ""
    refresh_token: str = ""
    expires_in: int = 1200
    token_type: str = "Bearer"
    scope: str = "CompanyFile"
    uid: Optional[str] = None  # MYOB user UID
    expires_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# MYOBClient
# ---------------------------------------------------------------------------

class MYOBClient:
    """
    High-level wrapper around the MYOB AccountRight Live API.

    Usage
    -----
    >>> client = MYOBClient(client_id="…", client_secret="…")
    >>> client.set_token(token_set)
    >>> ledger = await client.get_general_ledger(company_file_id="…")
    """

    # MYOB OAuth2 endpoints
    AUTHORIZATION_URL = "https://secure.myob.com/oauth2/account/authorize"
    TOKEN_URL = "https://secure.myob.com/oauth2/v1/authorize"
    API_BASE_URL = "https://api.myob.com/accountright"

    SCOPES = ["CompanyFile"]

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str = "http://localhost:8000/integrations/myob/callback",
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self._token: Optional[MYOBTokenSet] = None

        logger.info("MYOBClient initialised (client_id=%s…)", client_id[:8])

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------

    def get_authorization_url(self, state: str = "") -> str:
        """Build the MYOB OAuth2 authorization URL."""
        url = (
            f"{self.AUTHORIZATION_URL}"
            f"?client_id={self.client_id}"
            f"&redirect_uri={self.redirect_uri}"
            f"&response_type=code"
            f"&scope={' '.join(self.SCOPES)}"
            f"&state={state}"
        )
        logger.debug("MYOB Authorization URL: %s", url)
        return url

    async def exchange_code_for_token(self, authorization_code: str) -> MYOBTokenSet:
        """Exchange an authorization code for an access / refresh token pair."""
        logger.info("Exchanging MYOB authorization code for tokens (placeholder)")
        self._token = MYOBTokenSet(
            access_token="myob_placeholder_access_token",
            refresh_token="myob_placeholder_refresh_token",
            expires_in=1200,
        )
        return self._token

    async def refresh_access_token(self) -> MYOBTokenSet:
        """Refresh an expired access token."""
        if self._token is None:
            raise RuntimeError("No token set — authenticate first.")

        logger.info("Refreshing MYOB access token (placeholder)")
        self._token.access_token = "myob_refreshed_placeholder_access_token"
        return self._token

    def set_token(self, token: MYOBTokenSet) -> None:
        """Inject a previously-stored token set."""
        self._token = token

    async def get_company_files(self) -> list[dict[str, Any]]:
        """
        List available MYOB company files.

        Maps to: GET /accountright/
        """
        self._ensure_authenticated()
        logger.info("Fetching MYOB company files (placeholder)")
        return [
            {
                "Id": "placeholder-company-file-id",
                "Name": "Placeholder Company File",
                "LibraryPath": "Placeholder\\CompanyFile",
                "ProductVersion": "2024.0",
            }
        ]

    # ------------------------------------------------------------------
    # Accounting data extraction methods
    # ------------------------------------------------------------------

    async def get_general_ledger(
        self,
        company_file_id: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> list[Transaction]:
        """
        Retrieve general journal entries from a MYOB company file.

        Maps to: GET /accountright/{cf_id}/GeneralLedger/JournalTransaction
        """
        self._ensure_authenticated()
        logger.info(
            "Fetching MYOB general ledger for %s [placeholder]", company_file_id
        )

        return [
            Transaction(
                company_id="00000000-0000-0000-0000-000000000000",
                transaction_type=TransactionType.JOURNAL,
                reference="MYOB-JNL-0001",
                description="Placeholder MYOB journal entry",
                transaction_date=date.today(),
                amount=750.00,
                account_code="1-1100",
                account_name="General Expenses",
                source="myob",
                external_id="myob-journal-placeholder",
            )
        ]

    async def get_bank_transactions(
        self,
        company_file_id: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> list[Transaction]:
        """
        Retrieve bank transactions (spend/receive money) from a MYOB company file.

        Maps to:
            GET /accountright/{cf_id}/Banking/SpendMoneyTxn
            GET /accountright/{cf_id}/Banking/ReceiveMoneyTxn
        """
        self._ensure_authenticated()
        logger.info(
            "Fetching MYOB bank transactions for %s [placeholder]", company_file_id
        )

        return [
            Transaction(
                company_id="00000000-0000-0000-0000-000000000000",
                transaction_type=TransactionType.BANK_TRANSACTION,
                status=TransactionStatus.AUTHORISED,
                reference="MYOB-BT-0001",
                description="Placeholder MYOB bank transaction",
                transaction_date=date.today(),
                amount=320.00,
                currency="AUD",
                account_code="1-1100",
                account_name="Cheque Account",
                contact_name="Placeholder MYOB Vendor",
                source="myob",
                external_id="myob-bank-txn-placeholder",
            )
        ]

    async def get_aged_payables(
        self,
        company_file_id: str,
        as_at_date: Optional[date] = None,
    ) -> CreditorList:
        """
        Retrieve the aged payables (supplier balances) from a MYOB company file.

        Maps to: GET /accountright/{cf_id}/Report/PayableReconciliation/Summary
        """
        self._ensure_authenticated()
        report_date = as_at_date or date.today()
        logger.info(
            "Fetching MYOB aged payables for %s as at %s [placeholder]",
            company_file_id,
            report_date,
        )

        return CreditorList(
            company_id="00000000-0000-0000-0000-000000000000",
            as_at_date=report_date,
            creditors=[
                CreditorEntry(
                    creditor_name="MYOB Placeholder Supplier",
                    amount_claimed=18000.00,
                    category="unsecured",
                    contact_email="myob-supplier@example.com",
                    source_invoice_ids=["MYOB-INV-001"],
                ),
            ],
        )

    async def get_company_info(self, company_file_id: str) -> CompanyData:
        """
        Fetch the company profile from the MYOB company file.

        Maps to: GET /accountright/{cf_id}/Company
        """
        self._ensure_authenticated()
        logger.info("Fetching MYOB company info for %s [placeholder]", company_file_id)

        return CompanyData(
            legal_name="MYOB Placeholder Company Pty Ltd",
            trading_name="MYOB Placeholder",
            abn="98765432100",
            total_assets=800_000,
            total_liabilities=550_000,
            total_creditors=320_000,
            source="myob",
            external_id=company_file_id,
            last_synced_at=datetime.utcnow(),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_authenticated(self) -> None:
        """Raise if no valid token is present."""
        if self._token is None or not self._token.access_token:
            raise RuntimeError(
                "MYOBClient is not authenticated. "
                "Call exchange_code_for_token() or set_token() first."
            )
