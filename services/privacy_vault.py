"""
LexSolv AI — Privacy Vault: Anonymization & Re-identification Engine.

Protects sensitive data before it reaches Claude for analysis.
Uses tokenization to swap names, addresses, emails, phone numbers, and
financial identifiers for generic tokens (e.g. ENTITY_001, CREDITOR_A),
then maps them back once Claude returns its analysis.

Designed for financial JSON data from Xero / MYOB integrations:
  - Invoices
  - Transactions
  - Contacts (suppliers, creditors, customers)
  - Company profiles

Storage: In-memory dict by default, with automatic TTL-based expiry.
Each de-identification session gets a unique vault ID so multiple
concurrent analyses don't collide.
"""

from __future__ import annotations

import logging
import re
import secrets
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class SensitiveFieldCategory(str, Enum):
    """Categories of sensitive fields we detect and tokenize."""
    NAME = "name"
    ADDRESS = "address"
    EMAIL = "email"
    PHONE = "phone"
    ABN = "abn"
    BANK_ACCOUNT = "bank_account"
    TAX_NUMBER = "tax_number"


@dataclass
class VaultEntry:
    """A single mapping between a real value and its token."""
    token: str
    real_value: str
    category: SensitiveFieldCategory
    field_path: str  # JSON path where this value was found (audit trail)


@dataclass
class DeIdentificationMap:
    """The full de-identification map for one session."""
    vault_id: str
    created_at: datetime
    expires_at: datetime
    entity_count: int = 0
    entries: dict[str, VaultEntry] = field(default_factory=dict)      # token -> VaultEntry
    reverse_index: dict[str, str] = field(default_factory=dict)       # real_value -> token


@dataclass
class DeIdentificationResult:
    """Summary returned after de-identification."""
    vault_id: str
    sanitized_data: Any
    field_counts: dict[str, int]
    total_tokenized: int


# ---------------------------------------------------------------------------
# Field-detection configuration
# ---------------------------------------------------------------------------

# Maps field-name patterns to sensitivity categories.
# Matching is case-insensitive; checks if the normalised field name
# contains any of the listed substrings.
SENSITIVE_FIELD_PATTERNS: dict[SensitiveFieldCategory, list[str]] = {
    SensitiveFieldCategory.NAME: [
        "name", "firstname", "first_name", "lastname", "last_name",
        "contactname", "contact_name", "companyname", "company_name",
        "displayname", "display_name", "fullname", "full_name",
        "payeename", "payee_name", "payername", "payer_name",
        "vendorname", "vendor_name", "suppliername", "supplier_name",
        "directorname", "director_name", "creditorname", "creditor_name",
        "legal_name", "legalname", "trading_name", "tradingname",
        "practitioner_name", "practitionername", "firm_name", "firmname",
        "signer_name", "signername",
    ],
    SensitiveFieldCategory.ADDRESS: [
        "address", "addressline", "address_line", "street", "streetaddress",
        "street_address", "city", "suburb", "state", "postcode", "postalcode",
        "postal_code", "zipcode", "zip_code", "country",
        "deliveryaddress", "delivery_address", "postaladdress", "postal_address",
        "firm_address", "firmaddress",
    ],
    SensitiveFieldCategory.EMAIL: [
        "email", "emailaddress", "email_address",
        "contact_email", "contactemail",
        "firm_email", "firmemail",
    ],
    SensitiveFieldCategory.PHONE: [
        "phone", "phonenumber", "phone_number", "mobile", "mobilenumber",
        "mobile_number", "fax", "faxnumber", "fax_number", "telephone",
        "contact_phone", "contactphone",
        "firm_phone", "firmphone",
    ],
    SensitiveFieldCategory.ABN: [
        "abn", "acn", "businessnumber", "business_number",
    ],
    SensitiveFieldCategory.BANK_ACCOUNT: [
        "bankaccount", "bank_account", "accountnumber", "account_number",
        "bsb", "routingnumber", "routing_number", "iban",
        "swiftcode", "swift_code",
    ],
    SensitiveFieldCategory.TAX_NUMBER: [
        "tfn", "taxfilenumber", "tax_file_number",
        "vatnumber", "vat_number",
    ],
}

# Prefixes used when generating tokens — makes Claude's output human-readable.
# e.g. ENTITY_001, ADDRESS_003, CREDITOR_A
TOKEN_PREFIXES: dict[SensitiveFieldCategory, str] = {
    SensitiveFieldCategory.NAME: "ENTITY",
    SensitiveFieldCategory.ADDRESS: "ADDRESS",
    SensitiveFieldCategory.EMAIL: "EMAIL",
    SensitiveFieldCategory.PHONE: "PHONE",
    SensitiveFieldCategory.ABN: "ABN",
    SensitiveFieldCategory.BANK_ACCOUNT: "ACCOUNT",
    SensitiveFieldCategory.TAX_NUMBER: "TAXREF",
}

# Fields that should NEVER be tokenized, even if they match a pattern.
# These are non-sensitive structural fields whose names happen to contain
# a sensitive keyword (e.g. "account_code" contains "account").
FIELD_ALLOWLIST: set[str] = {
    "account_code", "accountcode",
    "account_name", "accountname",     # Chart-of-accounts label, not a person
    "source", "external_id", "externalid",
    "id", "company_id", "companyid",
    "transaction_type", "transactiontype",
    "status", "currency", "reference",
    "category", "description",         # Descriptions may contain names but are narrative
    "notes", "reason",
}


# ---------------------------------------------------------------------------
# In-memory vault store (module-level singleton)
# ---------------------------------------------------------------------------

_vault_store: dict[str, DeIdentificationMap] = {}
_store_lock = threading.Lock()

# Background cleanup of expired vaults
_cleanup_started = False


def _start_cleanup_thread() -> None:
    """Start a daemon thread that purges expired vaults every 60 seconds."""
    global _cleanup_started
    if _cleanup_started:
        return
    _cleanup_started = True

    def _cleanup_loop() -> None:
        while True:
            time.sleep(60)
            now = datetime.utcnow()
            with _store_lock:
                expired = [vid for vid, v in _vault_store.items() if v.expires_at < now]
                for vid in expired:
                    del _vault_store[vid]
                if expired:
                    logger.debug("Privacy vault cleanup: removed %d expired vault(s)", len(expired))

    t = threading.Thread(target=_cleanup_loop, daemon=True, name="privacy-vault-cleanup")
    t.start()


# ---------------------------------------------------------------------------
# DeIdentifier class
# ---------------------------------------------------------------------------

class DeIdentifier:
    """
    Scans financial JSON data and replaces sensitive fields with tokens.

    Usage
    -----
    >>> engine = DeIdentifier()
    >>> result = engine.de_identify(xero_contacts)
    >>> # result.sanitized_data is safe to send to Claude
    >>> # result.vault_id is needed later to re-identify
    """

    def __init__(
        self,
        ttl_seconds: int = 1800,
        extra_sensitive_fields: Optional[list[str]] = None,
        redact_mode: bool = False,
    ) -> None:
        """
        Parameters
        ----------
        ttl_seconds : int
            How long (seconds) the vault stays alive. Default 1800 (30 min).
        extra_sensitive_fields : list[str], optional
            Additional field names to treat as sensitive (case-insensitive).
        redact_mode : bool
            If True, replace with [REDACTED] instead of reversible tokens.
        """
        self.ttl_seconds = ttl_seconds
        self.extra_fields = [f.lower() for f in (extra_sensitive_fields or [])]
        self.redact_mode = redact_mode
        _start_cleanup_thread()

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    def de_identify(self, data: Any) -> DeIdentificationResult:
        """
        Scan financial JSON data and replace sensitive fields with tokens.

        Parameters
        ----------
        data : Any
            Raw financial JSON — dicts, lists of dicts, Pydantic .model_dump()
            output, etc.

        Returns
        -------
        DeIdentificationResult
            Contains sanitized_data (safe for Claude) and vault_id
            (needed for re-identification).
        """
        vault_id = f"vault_{secrets.token_hex(16)}"
        now = datetime.utcnow()

        vault = DeIdentificationMap(
            vault_id=vault_id,
            created_at=now,
            expires_at=now + timedelta(seconds=self.ttl_seconds),
        )

        field_counts: dict[str, int] = {cat.value: 0 for cat in SensitiveFieldCategory}

        # Deep-walk and tokenize
        sanitized = self._walk_and_tokenize(data, vault, field_counts, "$")

        # Store the vault
        with _store_lock:
            _vault_store[vault_id] = vault

        total = sum(field_counts.values())

        logger.info(
            "De-identified %d field(s) across %d unique entities → vault %s (expires %s)",
            total, vault.entity_count, vault_id, vault.expires_at.isoformat(),
        )

        return DeIdentificationResult(
            vault_id=vault_id,
            sanitized_data=sanitized,
            field_counts=field_counts,
            total_tokenized=total,
        )

    @staticmethod
    def get_vault(vault_id: str) -> Optional[DeIdentificationMap]:
        """Retrieve a vault by ID (for debugging / audit)."""
        with _store_lock:
            return _vault_store.get(vault_id)

    @staticmethod
    def destroy_vault(vault_id: str) -> bool:
        """Manually destroy a vault (e.g. after re-identification)."""
        with _store_lock:
            return _vault_store.pop(vault_id, None) is not None

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _walk_and_tokenize(
        self,
        node: Any,
        vault: DeIdentificationMap,
        counts: dict[str, int],
        path: str,
    ) -> Any:
        """Recursively walk data, replacing sensitive string values with tokens."""
        if node is None:
            return None

        if isinstance(node, list):
            return [
                self._walk_and_tokenize(item, vault, counts, f"{path}[{i}]")
                for i, item in enumerate(node)
            ]

        if isinstance(node, dict):
            result: dict[str, Any] = {}
            for key, value in node.items():
                field_path = f"{path}.{key}"
                category = self._classify_field(key)

                if category and isinstance(value, str) and value.strip():
                    token = self._get_or_create_token(value.strip(), category, field_path, vault)
                    counts[category.value] += 1
                    result[key] = token
                else:
                    result[key] = self._walk_and_tokenize(value, vault, counts, field_path)
            return result

        # Primitives (int, float, bool, str in non-sensitive context) pass through
        return node

    def _classify_field(self, field_name: str) -> Optional[SensitiveFieldCategory]:
        """Check if a field name matches any known sensitive category."""
        lower = field_name.lower()

        # Skip allowlisted structural fields
        normalised = re.sub(r"[^a-z0-9]", "", lower)
        if lower in FIELD_ALLOWLIST or normalised in FIELD_ALLOWLIST:
            return None

        # Check user-provided extra fields
        for extra in self.extra_fields:
            if extra in normalised:
                return SensitiveFieldCategory.NAME  # default to NAME

        # Check built-in patterns
        for category, patterns in SENSITIVE_FIELD_PATTERNS.items():
            for pattern in patterns:
                pat_normalised = re.sub(r"[^a-z0-9]", "", pattern.lower())
                if normalised == pat_normalised or pat_normalised in normalised:
                    return category

        return None

    def _get_or_create_token(
        self,
        real_value: str,
        category: SensitiveFieldCategory,
        field_path: str,
        vault: DeIdentificationMap,
    ) -> str:
        """Return existing token for a value, or create a new one."""
        # Consistent: same real value → same token within one session
        existing = vault.reverse_index.get(real_value)
        if existing:
            return existing

        if self.redact_mode:
            return "[REDACTED]"

        vault.entity_count += 1
        prefix = TOKEN_PREFIXES[category]
        token = f"{prefix}_{vault.entity_count:03d}"

        vault.entries[token] = VaultEntry(
            token=token,
            real_value=real_value,
            category=category,
            field_path=field_path,
        )
        vault.reverse_index[real_value] = token

        return token


# ---------------------------------------------------------------------------
# Re-Identifier — standalone function
# ---------------------------------------------------------------------------

def re_identify(
    analysis_output: Any,
    vault_id: str,
    destroy_after: bool = True,
) -> Any:
    """
    Takes Claude's analysis output (which uses tokens like ENTITY_001) and
    swaps the real names/values back in before saving to the final report.

    Works on any data shape — strings, dicts, lists, nested structures.
    Performs both:
      1. Exact token replacement in string fields
      2. Substring replacement inside longer text blocks (Claude's narrative)

    Parameters
    ----------
    analysis_output : Any
        Claude's analysis result (any JSON-serializable shape).
    vault_id : str
        The vault ID returned by DeIdentifier.de_identify().
    destroy_after : bool
        If True (default), destroy the vault after re-identification.

    Returns
    -------
    Any
        The analysis with all tokens replaced by real values.

    Raises
    ------
    ValueError
        If the vault ID is not found or has expired.
    """
    with _store_lock:
        vault = _vault_store.get(vault_id)

    if vault is None:
        raise ValueError(
            f'Privacy vault "{vault_id}" not found. '
            f"It may have expired or been destroyed."
        )

    if vault.expires_at < datetime.utcnow():
        with _store_lock:
            _vault_store.pop(vault_id, None)
        raise ValueError(
            f'Privacy vault "{vault_id}" has expired. '
            f"De-identification map is no longer available."
        )

    # Build a sorted list of tokens (longest first) to avoid partial replacement
    token_pairs: list[tuple[str, str]] = sorted(
        [(entry.token, entry.real_value) for entry in vault.entries.values()],
        key=lambda pair: -len(pair[0]),
    )

    result = _walk_and_replace(analysis_output, token_pairs)

    logger.info(
        "Re-identified %d token(s) from vault %s",
        len(token_pairs), vault_id,
    )

    if destroy_after:
        with _store_lock:
            _vault_store.pop(vault_id, None)

    return result


def _walk_and_replace(node: Any, token_pairs: list[tuple[str, str]]) -> Any:
    """Recursively walk data and replace tokens with real values."""
    if node is None:
        return None

    if isinstance(node, str):
        return _replace_tokens_in_string(node, token_pairs)

    if isinstance(node, list):
        return [_walk_and_replace(item, token_pairs) for item in node]

    if isinstance(node, dict):
        return {k: _walk_and_replace(v, token_pairs) for k, v in node.items()}

    return node


def _replace_tokens_in_string(text: str, token_pairs: list[tuple[str, str]]) -> str:
    """Replace all token occurrences in a string (exact + substring)."""
    result = text
    for token, real_value in token_pairs:
        if token in result:
            result = result.replace(token, real_value)
    return result


# ---------------------------------------------------------------------------
# Convenience helpers for common financial data shapes
# ---------------------------------------------------------------------------

def de_identify_contacts(
    contacts: list[dict[str, Any]],
    **kwargs: Any,
) -> DeIdentificationResult:
    """
    De-identify an array of Xero/MYOB-style contact objects.

    Contacts typically have: Name, FirstName, LastName, EmailAddress,
    Phones, Addresses, etc.
    """
    engine = DeIdentifier(**kwargs)
    return engine.de_identify(contacts)


def de_identify_invoices(
    invoices: list[dict[str, Any]],
    **kwargs: Any,
) -> DeIdentificationResult:
    """
    De-identify an array of Xero/MYOB-style invoice objects.

    Invoices embed Contact info, line items with descriptions, etc.
    """
    engine = DeIdentifier(**kwargs)
    return engine.de_identify(invoices)


def de_identify_transactions(
    transactions: list[dict[str, Any]],
    **kwargs: Any,
) -> DeIdentificationResult:
    """
    De-identify an array of transaction/journal objects.
    """
    engine = DeIdentifier(**kwargs)
    return engine.de_identify(transactions)


# ---------------------------------------------------------------------------
# Vault stats (for monitoring / admin dashboard)
# ---------------------------------------------------------------------------

def get_vault_stats() -> dict[str, Any]:
    """Return aggregate stats about active vaults."""
    with _store_lock:
        now = datetime.utcnow()
        total_tokens = sum(len(v.entries) for v in _vault_store.values())
        ages = [(now - v.created_at).total_seconds() for v in _vault_store.values()]

        return {
            "active_vaults": len(_vault_store),
            "total_tokens_stored": total_tokens,
            "oldest_vault_age_seconds": max(ages) if ages else None,
        }
