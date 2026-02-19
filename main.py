"""
LexSolv AI — FastAPI backend.

Combines the original demo analysis endpoints with the new Xero / MYOB
accounting integration architecture.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import tempfile
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import sqlalchemy as sa
from fastapi import Depends, FastAPI, HTTPException, Query, UploadFile
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession

from integrations.xero_client import XeroClient, XeroTokenSet
from integrations.myob_client import MYOBClient, MYOBTokenSet
from models.schemas import (
    CompanyData,
    CreditorList,
    DIRRIRequest,
    DocumentOutputEntry,
    DocumentOutputListResponse,
    DocumentResponse,
    FirmProfile,
    ForensicReport,
    NarrativeSection,
    PreferencePaymentReport,
    RelatedPartyReport,
    SolvencyScore,
    Transaction,
)
from db.database import async_engine, Base, get_db, IS_SQLITE
from db.models import AssetDB, CompanyDB, CreditorDB, DocumentOutputDB, EntityMapDB, NarrativeDB, PlanParametersDB
from services.file_parser import FileParser
from services.creditor_schedule import CreditorScheduleService
from services.comparison_engine import ComparisonEngine
from services.payment_schedule import PaymentScheduleGenerator
from services.forensic_engine import ForensicAnalyzer
from services.document_generator import DocumentGenerator
from services.privacy_vault import DeIdentifier, re_identify, get_vault_stats

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("lexsolv")

# ---------------------------------------------------------------------------
# Configuration (from environment variables)
# ---------------------------------------------------------------------------

XERO_CLIENT_ID = os.getenv("XERO_CLIENT_ID", "")
XERO_CLIENT_SECRET = os.getenv("XERO_CLIENT_SECRET", "")
XERO_REDIRECT_URI = os.getenv("XERO_REDIRECT_URI", "http://localhost:8000/integrations/xero/callback")

MYOB_CLIENT_ID = os.getenv("MYOB_CLIENT_ID", "")
MYOB_CLIENT_SECRET = os.getenv("MYOB_CLIENT_SECRET", "")
MYOB_REDIRECT_URI = os.getenv("MYOB_REDIRECT_URI", "http://localhost:8000/integrations/myob/callback")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")

# ---------------------------------------------------------------------------
# Integration client singletons
# ---------------------------------------------------------------------------

xero_client: Optional[XeroClient] = None
myob_client: Optional[MYOBClient] = None

# In-memory token / state storage (swap for DB / Redis in production)
_oauth_states: dict[str, str] = {}  # state -> provider

# Service singletons for SBR endpoints
file_parser = FileParser()
creditor_schedule_service = CreditorScheduleService()
comparison_engine = ComparisonEngine()
payment_schedule_generator = PaymentScheduleGenerator()

_ALLOWED_UPLOAD_EXTENSIONS = {".csv", ".xlsx", ".xls"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise database and integration clients on startup."""
    global xero_client, myob_client

    # --- Database ---------------------------------------------------------
    # Import all ORM models so Base.metadata knows about them
    import db.models  # noqa: F401

    if async_engine is not None:
        try:
            db_label = "SQLite" if IS_SQLITE else "PostgreSQL"
            logger.info("Connecting to %s…", db_label)
            async with async_engine.begin() as conn:
                # Enable foreign keys for SQLite
                if IS_SQLITE:
                    await conn.execute(sa.text("PRAGMA foreign_keys = ON"))
                # In production, use Alembic migrations instead of create_all.
                # This is a safety net for local development.
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables verified (%s)", db_label)
        except Exception as exc:
            logger.error("Could not connect to database: %s", exc)
            logger.warning("App will start without database — data endpoints will return errors")
    else:
        logger.warning("DATABASE_URL not set — running without database")

    # --- Integration clients -----------------------------------------------
    if XERO_CLIENT_ID:
        xero_client = XeroClient(
            client_id=XERO_CLIENT_ID,
            client_secret=XERO_CLIENT_SECRET,
            redirect_uri=XERO_REDIRECT_URI,
        )
        logger.info("Xero integration client ready")
    else:
        logger.warning("XERO_CLIENT_ID not set — Xero integration disabled")

    if MYOB_CLIENT_ID:
        myob_client = MYOBClient(
            client_id=MYOB_CLIENT_ID,
            client_secret=MYOB_CLIENT_SECRET,
            redirect_uri=MYOB_REDIRECT_URI,
        )
        logger.info("MYOB integration client ready")
    else:
        logger.warning("MYOB_CLIENT_ID not set — MYOB integration disabled")

    yield  # Application runs here

    # --- Shutdown ----------------------------------------------------------
    if async_engine is not None:
        await async_engine.dispose()
    logger.info("Shutting down LexSolv AI backend")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="LexSolv AI",
    description="Insolvency management platform — accounting integration backend",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===================================================================
# Original demo / analysis endpoints (preserved)
# ===================================================================

simulated_db = {"is_analyzing": False}


@app.get("/api/analysis", tags=["Analysis"])
async def get_analysis():
    if not simulated_db["is_analyzing"]:
        return {"status": "pending"}
    return {
        "status": "complete",
        "company_name": "Construction Corp Pty Ltd",
        "health_score": 42,
        "alerts": [
            {"type": "Critical", "msg": "Unpaid Superannuation: $12,400 (Safe Harbor Void)"},
            {"type": "Warning", "msg": "Preference Payment detected: $50k to 'Related Party'"},
            {"type": "Action", "msg": "ATO Debt: $185k - Payment plan recommended"},
        ],
        "forensic_matches": "94% (1,240/1,300 transactions reconciled)",
    }


@app.post("/api/trigger-analysis", tags=["Analysis"])
async def trigger():
    simulated_db["is_analyzing"] = True
    return {"status": "started"}


@app.post("/api/reset", tags=["Analysis"])
async def reset():
    simulated_db["is_analyzing"] = False
    return {"status": "reset"}


# ===================================================================
# Health check
# ===================================================================

@app.get("/health", tags=["System"])
@app.get("/api/health", tags=["System"])
async def health_check():
    # Quick DB connectivity check
    db_status = "not configured"
    if async_engine is not None:
        try:
            async with async_engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1"))
            db_status = "connected"
        except Exception:
            db_status = "unavailable"

    return {
        "status": "ok",
        "service": "lexsolv-ai",
        "database": db_status,
        "integrations": {
            "xero": "configured" if xero_client else "not configured",
            "myob": "configured" if myob_client else "not configured",
        },
    }


# ===================================================================
# Xero OAuth2 routes
# ===================================================================

@app.get("/integrations/xero/connect", tags=["Xero"])
async def xero_connect():
    """
    Initiate the Xero OAuth2 authorization flow.
    Redirects the user to the Xero login / consent screen.
    """
    if not xero_client:
        raise HTTPException(status_code=503, detail="Xero integration is not configured")

    state = secrets.token_urlsafe(32)
    _oauth_states[state] = "xero"
    authorization_url = xero_client.get_authorization_url(state=state)
    return RedirectResponse(url=authorization_url)


@app.get("/integrations/xero/callback", tags=["Xero"])
async def xero_callback(
    code: str = Query(..., description="Authorization code from Xero"),
    state: str = Query("", description="CSRF state parameter"),
):
    """
    OAuth2 callback endpoint for Xero.
    Exchanges the authorization code for tokens and stores them.
    """
    if not xero_client:
        raise HTTPException(status_code=503, detail="Xero integration is not configured")

    # Validate state to prevent CSRF
    if state and _oauth_states.pop(state, None) != "xero":
        raise HTTPException(status_code=400, detail="Invalid OAuth state parameter")

    try:
        token_set = await xero_client.exchange_code_for_token(code)
        connections = await xero_client.get_tenant_connections()
        return {
            "status": "connected",
            "provider": "xero",
            "tenant_count": len(connections),
            "tenants": connections,
            "message": "Xero integration connected successfully.",
        }
    except Exception as exc:
        logger.exception("Xero OAuth callback failed")
        raise HTTPException(status_code=400, detail=f"Xero authentication failed: {exc}")


@app.get("/integrations/xero/disconnect", tags=["Xero"])
async def xero_disconnect():
    """Revoke the Xero token and clear local state."""
    if xero_client:
        xero_client.set_token(XeroTokenSet())
    return {"status": "disconnected", "provider": "xero"}


# ===================================================================
# MYOB OAuth2 routes
# ===================================================================

@app.get("/integrations/myob/connect", tags=["MYOB"])
async def myob_connect():
    """
    Initiate the MYOB OAuth2 authorization flow.
    Redirects the user to the MYOB login / consent screen.
    """
    if not myob_client:
        raise HTTPException(status_code=503, detail="MYOB integration is not configured")

    state = secrets.token_urlsafe(32)
    _oauth_states[state] = "myob"
    authorization_url = myob_client.get_authorization_url(state=state)
    return RedirectResponse(url=authorization_url)


@app.get("/integrations/myob/callback", tags=["MYOB"])
async def myob_callback(
    code: str = Query(..., description="Authorization code from MYOB"),
    state: str = Query("", description="CSRF state parameter"),
):
    """
    OAuth2 callback endpoint for MYOB.
    Exchanges the authorization code for tokens and stores them.
    """
    if not myob_client:
        raise HTTPException(status_code=503, detail="MYOB integration is not configured")

    if state and _oauth_states.pop(state, None) != "myob":
        raise HTTPException(status_code=400, detail="Invalid OAuth state parameter")

    try:
        token_set = await myob_client.exchange_code_for_token(code)
        company_files = await myob_client.get_company_files()
        return {
            "status": "connected",
            "provider": "myob",
            "company_file_count": len(company_files),
            "company_files": company_files,
            "message": "MYOB integration connected successfully.",
        }
    except Exception as exc:
        logger.exception("MYOB OAuth callback failed")
        raise HTTPException(status_code=400, detail=f"MYOB authentication failed: {exc}")


@app.get("/integrations/myob/disconnect", tags=["MYOB"])
async def myob_disconnect():
    """Revoke the MYOB token and clear local state."""
    if myob_client:
        myob_client.set_token(MYOBTokenSet())
    return {"status": "disconnected", "provider": "myob"}


# ===================================================================
# Xero data extraction endpoints
# ===================================================================

@app.get(
    "/integrations/xero/{tenant_id}/general-ledger",
    response_model=list[Transaction],
    tags=["Xero"],
)
async def xero_general_ledger(tenant_id: str):
    """Fetch the general ledger from Xero for a given tenant."""
    if not xero_client:
        raise HTTPException(status_code=503, detail="Xero integration is not configured")
    try:
        return await xero_client.get_general_ledger(tenant_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@app.get(
    "/integrations/xero/{tenant_id}/bank-transactions",
    response_model=list[Transaction],
    tags=["Xero"],
)
async def xero_bank_transactions(tenant_id: str):
    """Fetch bank transactions from Xero for a given tenant."""
    if not xero_client:
        raise HTTPException(status_code=503, detail="Xero integration is not configured")
    try:
        return await xero_client.get_bank_transactions(tenant_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@app.get(
    "/integrations/xero/{tenant_id}/aged-payables",
    response_model=CreditorList,
    tags=["Xero"],
)
async def xero_aged_payables(tenant_id: str):
    """Fetch the aged payables report from Xero for a given tenant."""
    if not xero_client:
        raise HTTPException(status_code=503, detail="Xero integration is not configured")
    try:
        return await xero_client.get_aged_payables(tenant_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@app.get(
    "/integrations/xero/{tenant_id}/organisation",
    response_model=CompanyData,
    tags=["Xero"],
)
async def xero_organisation(tenant_id: str):
    """Fetch the organisation profile from Xero for a given tenant."""
    if not xero_client:
        raise HTTPException(status_code=503, detail="Xero integration is not configured")
    try:
        return await xero_client.get_organisation(tenant_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


# ===================================================================
# MYOB data extraction endpoints
# ===================================================================

@app.get(
    "/integrations/myob/{company_file_id}/general-ledger",
    response_model=list[Transaction],
    tags=["MYOB"],
)
async def myob_general_ledger(company_file_id: str):
    """Fetch the general ledger from a MYOB company file."""
    if not myob_client:
        raise HTTPException(status_code=503, detail="MYOB integration is not configured")
    try:
        return await myob_client.get_general_ledger(company_file_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@app.get(
    "/integrations/myob/{company_file_id}/bank-transactions",
    response_model=list[Transaction],
    tags=["MYOB"],
)
async def myob_bank_transactions(company_file_id: str):
    """Fetch bank transactions from a MYOB company file."""
    if not myob_client:
        raise HTTPException(status_code=503, detail="MYOB integration is not configured")
    try:
        return await myob_client.get_bank_transactions(company_file_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@app.get(
    "/integrations/myob/{company_file_id}/aged-payables",
    response_model=CreditorList,
    tags=["MYOB"],
)
async def myob_aged_payables(company_file_id: str):
    """Fetch the aged payables from a MYOB company file."""
    if not myob_client:
        raise HTTPException(status_code=503, detail="MYOB integration is not configured")
    try:
        return await myob_client.get_aged_payables(company_file_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@app.get(
    "/integrations/myob/{company_file_id}/company",
    response_model=CompanyData,
    tags=["MYOB"],
)
async def myob_company_info(company_file_id: str):
    """Fetch the company profile from a MYOB company file."""
    if not myob_client:
        raise HTTPException(status_code=503, detail="MYOB integration is not configured")
    try:
        return await myob_client.get_company_info(company_file_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=401, detail=str(exc))


# ===================================================================
# Forensic Analysis endpoints
# ===================================================================

# Instantiate the forensic engine (stateless — safe as singleton)
forensic_analyzer = ForensicAnalyzer()


class ForensicRequest(BaseModel):
    """Request body for a full forensic analysis."""

    transactions: list[Transaction]
    insolvency_date: date
    director_names: list[str] = []
    current_assets: Decimal = Decimal("0.00")
    current_liabilities: Decimal = Decimal("0.00")
    company_name: Optional[str] = None
    threshold_days: int = 90


class PreferencePaymentRequest(BaseModel):
    """Request body for preference-payment detection only."""

    transactions: list[Transaction]
    insolvency_date: date
    threshold_days: int = 90


class RelatedPartyRequest(BaseModel):
    """Request body for related-party detection only."""

    transactions: list[Transaction]
    director_names: list[str]


class SolvencyRequest(BaseModel):
    """Request body for solvency score calculation only."""

    current_assets: Decimal
    current_liabilities: Decimal


@app.post(
    "/api/forensic/analyze",
    response_model=ForensicReport,
    tags=["Forensic"],
    summary="Run full forensic analysis",
)
async def forensic_full_analysis(req: ForensicRequest):
    """
    Run the complete forensic analysis: preference payments, related-party
    transactions, and solvency score. Returns a single dashboard-ready report.
    """
    return forensic_analyzer.full_report(
        transactions=req.transactions,
        insolvency_date=req.insolvency_date,
        director_names=req.director_names,
        current_assets=req.current_assets,
        current_liabilities=req.current_liabilities,
        company_name=req.company_name,
        threshold_days=req.threshold_days,
    )


@app.post(
    "/api/forensic/preference-payments",
    response_model=PreferencePaymentReport,
    tags=["Forensic"],
    summary="Detect preference payments",
)
async def forensic_preference_payments(req: PreferencePaymentRequest):
    """
    Scan transactions for potential unfair preference payments made within
    the look-back window before the insolvency date.
    """
    return forensic_analyzer.detect_preference_payments(
        transactions=req.transactions,
        insolvency_date=req.insolvency_date,
        threshold_days=req.threshold_days,
    )


@app.post(
    "/api/forensic/related-parties",
    response_model=RelatedPartyReport,
    tags=["Forensic"],
    summary="Identify related-party transactions",
)
async def forensic_related_parties(req: RelatedPartyRequest):
    """
    Scan transactions for payees or descriptions matching director names
    or known related parties.
    """
    return forensic_analyzer.identify_related_parties(
        transactions=req.transactions,
        director_names_list=req.director_names,
    )


@app.post(
    "/api/forensic/solvency-score",
    response_model=SolvencyScore,
    tags=["Forensic"],
    summary="Calculate solvency score (Liquidation vs SBR)",
)
async def forensic_solvency_score(req: SolvencyRequest):
    """
    Calculate the Liquidation vs Small Business Restructuring (SBR) ratio
    and return a 0-100 solvency score with a recommendation.
    """
    return forensic_analyzer.calculate_solvency_score(
        current_assets=req.current_assets,
        current_liabilities=req.current_liabilities,
    )


# ===================================================================
# Document Generation endpoints
# ===================================================================

document_generator = DocumentGenerator()


class SafeHarbourRequest(BaseModel):
    """Request body for generating a Safe Harbour assessment checklist."""

    firm_profile: FirmProfile
    company: CompanyData
    assessment_date: Optional[date] = None


@app.post(
    "/api/documents/dirri",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="Generate a DIRRI document",
)
async def generate_dirri(req: DIRRIRequest):
    """
    Generate a draft Declaration of Independence, Relevant Relationships
    and Indemnities (DIRRI) report in .docx format.

    The document follows the ARITA standard template and includes
    review/sign-off placeholders for the Registered Liquidator.
    """
    from datetime import datetime as dt

    filepath = document_generator.generate_dirri(req)
    return DocumentResponse(
        filename=filepath.name,
        document_type="DIRRI",
        download_url=f"/documents/{filepath.name}",
        generated_at=dt.utcnow(),
        company_name=req.company.legal_name,
        practitioner_name=req.firm_profile.practitioner_name,
    )


@app.post(
    "/api/documents/safe-harbour",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="Generate a Safe Harbour assessment checklist",
)
async def generate_safe_harbour(req: SafeHarbourRequest):
    """
    Generate a Safe Harbour (Section 588GA) assessment checklist
    in .docx format. Includes a table of conditions and recommendation
    placeholders.
    """
    from datetime import datetime as dt

    filepath = document_generator.generate_safe_harbour_checklist(
        firm=req.firm_profile,
        company=req.company,
        assessment_date=req.assessment_date,
    )
    return DocumentResponse(
        filename=filepath.name,
        document_type="Safe Harbour Assessment",
        download_url=f"/documents/{filepath.name}",
        generated_at=dt.utcnow(),
        company_name=req.company.legal_name,
        practitioner_name=req.firm_profile.practitioner_name,
    )


# ===================================================================
# Privacy Vault endpoints (Anonymization & Re-identification)
# ===================================================================

privacy_engine = DeIdentifier()


class DeIdentifyRequest(BaseModel):
    """Request body for de-identifying financial data before Claude analysis."""

    data: list[dict] | dict = []
    ttl_seconds: int = 1800
    extra_sensitive_fields: list[str] = []
    redact_mode: bool = False


class ReIdentifyRequest(BaseModel):
    """Request body for re-identifying Claude's analysis output."""

    vault_id: str
    analysis_output: dict | list | str
    destroy_after: bool = True


@app.post(
    "/api/privacy/de-identify",
    tags=["Privacy Vault"],
    summary="De-identify sensitive data before sending to Claude",
)
async def de_identify_data(req: DeIdentifyRequest):
    """
    Scan financial JSON data (invoices, transactions, contacts) and replace
    sensitive fields (names, addresses, emails, phones, ABNs) with generic
    tokens like ENTITY_001, ADDRESS_002, etc.

    Returns the sanitized data (safe for Claude) and a vault_id needed
    to re-identify the real values later.
    """
    engine = DeIdentifier(
        ttl_seconds=req.ttl_seconds,
        extra_sensitive_fields=req.extra_sensitive_fields if req.extra_sensitive_fields else None,
        redact_mode=req.redact_mode,
    )
    result = engine.de_identify(req.data)
    return {
        "vault_id": result.vault_id,
        "sanitized_data": result.sanitized_data,
        "field_counts": result.field_counts,
        "total_tokenized": result.total_tokenized,
    }


@app.post(
    "/api/privacy/re-identify",
    tags=["Privacy Vault"],
    summary="Re-identify tokens in Claude's analysis with real values",
)
async def re_identify_data(req: ReIdentifyRequest):
    """
    Takes Claude's analysis output (which uses tokens like ENTITY_001)
    and swaps the real names/values back in before saving to the final report.
    """
    try:
        restored = re_identify(
            analysis_output=req.analysis_output,
            vault_id=req.vault_id,
            destroy_after=req.destroy_after,
        )
        return {
            "status": "success",
            "restored_data": restored,
        }
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get(
    "/api/privacy/vault-stats",
    tags=["Privacy Vault"],
    summary="Get privacy vault statistics",
)
async def vault_stats():
    """Return aggregate stats about active de-identification vaults."""
    return get_vault_stats()


# ===================================================================
# File Upload endpoints (1.4A)
# ===================================================================


def _validate_upload_extension(filename: str | None) -> str:
    """Validate file extension and return the lowercased extension. Raises 400 on failure."""
    if not filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    ext = os.path.splitext(filename)[1].lower()
    if ext not in _ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail="Supported formats: .csv, .xlsx, .xls",
        )
    return ext


async def _save_upload_to_temp(file: UploadFile, ext: str) -> str:
    """Save UploadFile content to a temp file, return its path."""
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
    try:
        tmp.write(content)
        tmp.flush()
        return tmp.name
    finally:
        tmp.close()


@app.post("/api/upload/aged-payables", tags=["File Upload"])
async def upload_aged_payables(file: UploadFile):
    """
    Upload aged payables CSV/Excel and return parsed creditor list
    with auto-classification.
    """
    ext = _validate_upload_extension(file.filename)
    tmp_path = await _save_upload_to_temp(file, ext)
    try:
        parsed = file_parser.parse_aged_payables(tmp_path)
        creditors = creditor_schedule_service.build_from_parsed(parsed)
        # Detect parse method from column mapping
        parse_method = "generic"
        if any(c.get("category", "").startswith("ato") for c in creditors):
            parse_method = "xero"  # best guess — Xero uses "Contact" column
        return {
            "creditors": creditors,
            "count": len(creditors),
            "parse_method": parse_method,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        os.unlink(tmp_path)


@app.post("/api/upload/balance-sheet", tags=["File Upload"])
async def upload_balance_sheet(file: UploadFile):
    """
    Upload balance sheet and return parsed asset register with default
    recovery rates.
    """
    ext = _validate_upload_extension(file.filename)
    tmp_path = await _save_upload_to_temp(file, ext)
    try:
        parsed = file_parser.parse_balance_sheet(tmp_path)
        assets = comparison_engine.build_assets_from_balance_sheet(parsed)
        return {
            "assets": assets,
            "total_liabilities": parsed.get("total_liabilities", 0.0),
            "parse_method": "keyword_match",
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        os.unlink(tmp_path)


@app.post("/api/upload/bank-statement", tags=["File Upload"])
async def upload_bank_statement(file: UploadFile):
    """Upload bank statement CSV and return closing balance and period."""
    ext = _validate_upload_extension(file.filename)
    tmp_path = await _save_upload_to_temp(file, ext)
    try:
        result = file_parser.parse_bank_statement(tmp_path)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        os.unlink(tmp_path)


@app.post("/api/upload/pnl", tags=["File Upload"])
async def upload_pnl(file: UploadFile):
    """Upload P&L and return revenue/profit history."""
    ext = _validate_upload_extension(file.filename)
    tmp_path = await _save_upload_to_temp(file, ext)
    try:
        result = file_parser.parse_pnl(tmp_path)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        os.unlink(tmp_path)


# ===================================================================
# SBR Engagement endpoints (1.4B)
# ===================================================================


@app.post("/api/engagements", tags=["SBR Engagement"])
async def create_engagement(data: dict, db: AsyncSession = Depends(get_db)):
    """
    Create new SBR engagement. Creates a company record if needed.
    Body: {"company_name": "...", "acn": "...", "abn": "...",
           "appointment_date": "...", "practitioner_name": "..."}
    Returns the company record with ID.
    """
    company = CompanyDB(
        legal_name=data.get("company_name", ""),
        acn=data.get("acn"),
        abn=data.get("abn"),
        source="sbr",
    )
    db.add(company)
    await db.flush()
    return {
        "id": str(company.id),
        "company_name": company.legal_name,
        "acn": company.acn,
        "abn": company.abn,
        "appointment_date": data.get("appointment_date"),
        "practitioner_name": data.get("practitioner_name"),
        "created_at": str(company.created_at) if company.created_at else None,
    }


@app.get("/api/engagements/{company_id}", tags=["SBR Engagement"])
async def get_engagement(company_id: str, db: AsyncSession = Depends(get_db)):
    """Get engagement with all related data: company, creditors, assets, plan parameters."""
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Load related data
    cred_result = await db.execute(
        sa.select(CreditorDB).where(CreditorDB.company_id == cid)
    )
    creditors = cred_result.scalars().all()

    asset_result = await db.execute(
        sa.select(AssetDB).where(AssetDB.company_id == cid)
    )
    assets = asset_result.scalars().all()

    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan = plan_result.scalar_one_or_none()

    return {
        "company": {
            "id": str(company.id),
            "legal_name": company.legal_name,
            "acn": company.acn,
            "abn": company.abn,
        },
        "creditors": [
            {
                "id": str(c.id),
                "creditor_name": c.creditor_name,
                "amount_claimed": float(c.amount_claimed),
                "category": c.category,
                "status": c.status,
                "is_related_party": c.is_related_party,
                "is_secured": c.is_secured,
                "can_vote": c.can_vote,
                "notes": c.notes,
            }
            for c in creditors
        ],
        "assets": [
            {
                "id": str(a.id),
                "asset_type": a.asset_type,
                "description": a.description,
                "book_value": a.book_value,
                "liquidation_recovery_pct": a.liquidation_recovery_pct,
                "liquidation_value": a.liquidation_value,
            }
            for a in assets
        ],
        "plan_parameters": (
            {
                "total_contribution": plan.total_contribution,
                "practitioner_fee_pct": plan.practitioner_fee_pct,
                "num_initial_payments": plan.num_initial_payments,
                "initial_payment_amount": plan.initial_payment_amount,
                "num_ongoing_payments": plan.num_ongoing_payments,
                "ongoing_payment_amount": plan.ongoing_payment_amount,
                "est_liquidator_fees": plan.est_liquidator_fees,
                "est_legal_fees": plan.est_legal_fees,
                "est_disbursements": plan.est_disbursements,
            }
            if plan
            else None
        ),
    }


@app.patch("/api/engagements/{company_id}/plan", tags=["SBR Engagement"])
async def update_plan_parameters(
    company_id: str, plan: dict, db: AsyncSession = Depends(get_db)
):
    """Create or update plan parameters for an engagement."""
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Verify company exists
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Check for existing plan
    result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    existing = result.scalar_one_or_none()

    if existing:
        for key, value in plan.items():
            if hasattr(existing, key):
                setattr(existing, key, value)
    else:
        existing = PlanParametersDB(company_id=cid, **plan)
        db.add(existing)

    await db.flush()
    return {
        "status": "updated",
        "plan_parameters": {
            "total_contribution": existing.total_contribution,
            "practitioner_fee_pct": existing.practitioner_fee_pct,
            "num_initial_payments": existing.num_initial_payments,
            "initial_payment_amount": existing.initial_payment_amount,
            "num_ongoing_payments": existing.num_ongoing_payments,
            "ongoing_payment_amount": existing.ongoing_payment_amount,
            "est_liquidator_fees": existing.est_liquidator_fees,
            "est_legal_fees": existing.est_legal_fees,
            "est_disbursements": existing.est_disbursements,
        },
    }


@app.post("/api/engagements/{company_id}/creditors", tags=["SBR Engagement"])
async def save_creditors(
    company_id: str,
    creditors: list[dict],
    db: AsyncSession = Depends(get_db),
):
    """
    Save creditor schedule. Replaces existing creditors for this company.
    Accepts the reviewed/adjusted creditor list from the UI.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Verify company exists
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Delete existing creditors
    await db.execute(
        sa.delete(CreditorDB).where(CreditorDB.company_id == cid)
    )

    # Insert new creditors
    saved = []
    for c in creditors:
        cred = CreditorDB(
            company_id=cid,
            creditor_name=c["creditor_name"],
            amount_claimed=c["amount_claimed"],
            category=c.get("category"),
            status=c.get("status", "active"),
            is_related_party=c.get("is_related_party", False),
            is_secured=c.get("is_secured", False),
            can_vote=c.get("can_vote", True),
            notes=c.get("notes"),
            source=c.get("source", "manual"),
        )
        db.add(cred)
        await db.flush()
        saved.append({"id": str(cred.id), "creditor_name": cred.creditor_name})

    return {"status": "saved", "count": len(saved), "creditors": saved}


@app.patch(
    "/api/engagements/{company_id}/creditors/{creditor_id}",
    tags=["SBR Engagement"],
)
async def update_creditor(
    company_id: str,
    creditor_id: str,
    updates: dict,
    db: AsyncSession = Depends(get_db),
):
    """Update a single creditor: related-party flag, status, amount, notes, etc."""
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
        cred_id = _uuid.UUID(creditor_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    result = await db.execute(
        sa.select(CreditorDB).where(
            CreditorDB.id == cred_id, CreditorDB.company_id == cid
        )
    )
    creditor = result.scalar_one_or_none()
    if not creditor:
        raise HTTPException(status_code=404, detail="Creditor not found")

    allowed_fields = {
        "creditor_name", "amount_claimed", "category", "status",
        "is_related_party", "is_secured", "can_vote", "notes",
    }
    for key, value in updates.items():
        if key in allowed_fields and hasattr(creditor, key):
            setattr(creditor, key, value)

    await db.flush()
    return {
        "status": "updated",
        "creditor": {
            "id": str(creditor.id),
            "creditor_name": creditor.creditor_name,
            "amount_claimed": float(creditor.amount_claimed),
            "is_related_party": creditor.is_related_party,
            "can_vote": creditor.can_vote,
        },
    }


@app.post("/api/engagements/{company_id}/assets", tags=["SBR Engagement"])
async def save_assets(
    company_id: str,
    assets: list[dict],
    total_liabilities: float | None = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Save asset register. Replaces existing assets for this company.
    Optionally pass ?total_liabilities=985777.37 to store the balance-sheet
    total liabilities on the company record for use in comparison calculations.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Verify company exists
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Store total_liabilities on company if provided
    if total_liabilities is not None and total_liabilities > 0:
        company.total_creditors = total_liabilities
        await db.flush()

    # Delete existing assets
    await db.execute(
        sa.delete(AssetDB).where(AssetDB.company_id == cid)
    )

    # Insert new assets
    saved = []
    for a in assets:
        asset = AssetDB(
            company_id=cid,
            asset_type=a.get("asset_type"),
            description=a.get("description"),
            book_value=a.get("book_value"),
            liquidation_recovery_pct=a.get("liquidation_recovery_pct"),
            liquidation_value=a.get("liquidation_value"),
            notes=a.get("notes"),
            source=a.get("source", "manual"),
        )
        db.add(asset)
        await db.flush()
        saved.append({"id": str(asset.id), "asset_type": asset.asset_type})

    return {"status": "saved", "count": len(saved), "assets": saved}


# ===================================================================
# SBR Calculation endpoints (1.4C)
# ===================================================================


@app.post("/api/engagements/{company_id}/compare", tags=["SBR Calculations"])
async def run_comparison(company_id: str, db: AsyncSession = Depends(get_db)):
    """
    Run SBR vs Liquidation comparison.
    Loads assets, creditors, and plan parameters from DB for this company.
    Returns ComparisonResult.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Load company (for total_creditors override)
    company_result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Load plan parameters
    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan_row = plan_result.scalar_one_or_none()
    if not plan_row:
        raise HTTPException(
            status_code=400,
            detail="Plan parameters required before comparison",
        )

    # Load creditors
    cred_result = await db.execute(
        sa.select(CreditorDB).where(CreditorDB.company_id == cid)
    )
    creditors = cred_result.scalars().all()
    if not creditors:
        raise HTTPException(status_code=400, detail="Creditor schedule required")

    # Load assets
    asset_result = await db.execute(
        sa.select(AssetDB).where(AssetDB.company_id == cid)
    )
    assets_rows = asset_result.scalars().all()
    if not assets_rows:
        raise HTTPException(status_code=400, detail="Asset register required")

    # Build dicts for ComparisonEngine
    assets = [
        {
            "asset_type": a.asset_type,
            "description": a.description,
            "book_value": a.book_value,
            "liquidation_recovery_pct": a.liquidation_recovery_pct,
            "liquidation_value": a.liquidation_value,
        }
        for a in assets_rows
    ]

    # Use balance-sheet total liabilities if stored, otherwise sum individual claims
    stored_total = float(company.total_creditors or 0)
    if stored_total > 0:
        creditors_total = stored_total
    else:
        creditors_total = sum(float(c.amount_claimed) for c in creditors)

    plan = {
        "total_contribution": plan_row.total_contribution,
        "practitioner_fee_pct": plan_row.practitioner_fee_pct,
        "est_liquidator_fees": plan_row.est_liquidator_fees,
        "est_legal_fees": plan_row.est_legal_fees,
        "est_disbursements": plan_row.est_disbursements,
    }

    result = comparison_engine.calculate(assets, creditors_total, plan)
    return result


@app.get(
    "/api/engagements/{company_id}/payment-schedule",
    tags=["SBR Calculations"],
)
async def get_payment_schedule(
    company_id: str, db: AsyncSession = Depends(get_db)
):
    """
    Generate payment schedule from stored plan parameters.
    Returns PaymentScheduleResult.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan_row = plan_result.scalar_one_or_none()
    if not plan_row:
        raise HTTPException(
            status_code=400,
            detail="Plan parameters required before generating payment schedule",
        )

    plan = {
        "total_contribution": plan_row.total_contribution,
        "practitioner_fee_pct": plan_row.practitioner_fee_pct,
        "num_initial_payments": plan_row.num_initial_payments,
        "initial_payment_amount": plan_row.initial_payment_amount,
        "num_ongoing_payments": plan_row.num_ongoing_payments,
        "ongoing_payment_amount": plan_row.ongoing_payment_amount,
    }

    try:
        result = payment_schedule_generator.generate(plan)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return result


# ===================================================================
# Narrative Generation endpoints (2.3)
# ===================================================================

VALID_SECTIONS = {
    "background", "distress_events", "expert_advice",
    "plan_summary", "viability", "comparison_commentary",
}

GLOSSARY_DIR = Path(__file__).resolve().parent / "data" / "glossaries"


class NarrativeGenerateRequest(BaseModel):
    """Request body for generating narrative sections."""
    director_notes: str
    industry: Optional[str] = None
    custom_terms: Optional[dict[str, str]] = None
    known_entities: Optional[dict[str, list[str]]] = None


class NarrativeSectionRequest(BaseModel):
    """Request body for single section regeneration."""
    director_notes: Optional[str] = None
    industry: Optional[str] = None
    custom_terms: Optional[dict[str, str]] = None
    known_entities: Optional[dict[str, list[str]]] = None


class NarrativePatchRequest(BaseModel):
    """Request body for updating/approving a narrative section."""
    content: Optional[str] = None
    status: Optional[str] = None


class CustomTermsRequest(BaseModel):
    """Request body for adding custom glossary terms."""
    terms: dict[str, str]


async def _load_engagement_data(
    db: AsyncSession, company_id, plan_row=None, comparison_result=None
) -> dict:
    """Build engagement_data dict from DB for narrative generation."""
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == company_id)
    )
    company = result.scalar_one_or_none()
    if not company:
        return {}

    data = {
        "company_name": company.legal_name,
        "acn": company.acn,
        "abn": company.abn,
    }

    if plan_row:
        data.update({
            "total_contribution": plan_row.total_contribution,
            "practitioner_fee_pct": plan_row.practitioner_fee_pct,
            "num_initial_payments": plan_row.num_initial_payments,
            "initial_payment_amount": plan_row.initial_payment_amount,
            "num_ongoing_payments": plan_row.num_ongoing_payments,
            "ongoing_payment_amount": plan_row.ongoing_payment_amount,
        })

    return data


@app.post("/api/engagements/{company_id}/narrative", tags=["Narrative"])
async def generate_narrative(
    company_id: str,
    req: NarrativeGenerateRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Generate all 6 narrative sections from director notes.
    Pipeline: validate → scrub PII → load engagement data → generate → store → return.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Validate director_notes
    if not req.director_notes or not req.director_notes.strip():
        raise HTTPException(status_code=400, detail="director_notes is required and cannot be empty")

    # Validate engagement exists
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Load plan parameters
    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan_row = plan_result.scalar_one_or_none()

    # Scrub PII from director notes
    from services.privacy_vault import scrub, restore

    # Build known entities from DB + request for more thorough PII scrubbing
    known = req.known_entities or {}
    if company.legal_name:
        known.setdefault("counterparty", [])
        if company.legal_name not in known["counterparty"]:
            known["counterparty"].append(company.legal_name)
    if getattr(company, "trading_name", None) and company.trading_name != company.legal_name:
        known.setdefault("counterparty", [])
        if company.trading_name not in known["counterparty"]:
            known["counterparty"].append(company.trading_name)

    scrub_result = scrub(req.director_notes, known_entities=known if known else None)

    # Build engagement data
    engagement_data = {
        "company_name": company.legal_name,
        "acn": company.acn,
        "abn": company.abn,
    }
    if plan_row:
        engagement_data.update({
            "total_contribution": plan_row.total_contribution,
            "practitioner_fee_pct": plan_row.practitioner_fee_pct,
            "num_initial_payments": plan_row.num_initial_payments,
            "initial_payment_amount": plan_row.initial_payment_amount,
            "num_ongoing_payments": plan_row.num_ongoing_payments,
            "ongoing_payment_amount": plan_row.ongoing_payment_amount,
        })

    # Load comparison data if available
    comparison_data = None
    if plan_row:
        try:
            # Load assets and creditors for comparison
            asset_result = await db.execute(
                sa.select(AssetDB).where(AssetDB.company_id == cid)
            )
            assets_rows = asset_result.scalars().all()

            cred_result = await db.execute(
                sa.select(CreditorDB).where(CreditorDB.company_id == cid)
            )
            creditors = cred_result.scalars().all()

            if assets_rows and creditors:
                assets = [
                    {
                        "asset_type": a.asset_type,
                        "description": a.description,
                        "book_value": a.book_value,
                        "liquidation_recovery_pct": a.liquidation_recovery_pct,
                        "liquidation_value": a.liquidation_value,
                    }
                    for a in assets_rows
                ]
                stored_total = float(company.total_creditors or 0)
                creditors_total = stored_total if stored_total > 0 else sum(float(c.amount_claimed) for c in creditors)
                plan = {
                    "total_contribution": plan_row.total_contribution,
                    "practitioner_fee_pct": plan_row.practitioner_fee_pct,
                    "est_liquidator_fees": plan_row.est_liquidator_fees,
                    "est_legal_fees": plan_row.est_legal_fees,
                    "est_disbursements": plan_row.est_disbursements,
                }
                comparison_data = comparison_engine.calculate(assets, creditors_total, plan)
        except Exception:
            logger.warning("Could not load comparison data for narrative generation")

    # Initialize Claude client and narrative generator
    try:
        from services.claude_client import ClaudeClient
        from services.narrative_generator import NarrativeGenerator

        claude_client = ClaudeClient()
        # Merge custom terms with engagement-level terms
        custom_terms = req.custom_terms or {}
        if company.custom_glossary:
            merged_terms = {**company.custom_glossary, **custom_terms}
        else:
            merged_terms = custom_terms

        generator = NarrativeGenerator(
            claude_client=claude_client,
            industry=req.industry,
            custom_terms=merged_terms or None,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=f"Claude API unavailable: {exc}")

    # Generate all 6 sections, handling partial failures
    section_generators = {
        "background": lambda: generator.generate_background(
            scrub_result.scrubbed_text, engagement_data
        ),
        "distress_events": lambda: generator.generate_distress_events(
            scrub_result.scrubbed_text, engagement_data
        ),
        "expert_advice": lambda: generator.generate_expert_advice(engagement_data),
        "plan_summary": lambda: generator.generate_plan_summary(
            engagement_data, comparison_data or {}
        ),
        "viability": lambda: generator.generate_viability(
            scrub_result.scrubbed_text, engagement_data
        ),
        "comparison_commentary": lambda: generator.generate_comparison_commentary(
            comparison_data or {}
        ),
    }

    sections = []
    for section_name, gen_func in section_generators.items():
        try:
            output = await gen_func()
            content = restore(output["content"], scrub_result.entity_map)
            metadata = output["metadata"]
            requires_input_flags = metadata.get("requires_input_flags", [])
            unknown_terms = metadata.get("unknown_terms_flagged", [])

            # Delete existing section for this engagement if any
            await db.execute(
                sa.delete(NarrativeDB).where(
                    NarrativeDB.engagement_id == cid,
                    NarrativeDB.section == section_name,
                )
            )

            # Store in NarrativeDB
            narrative = NarrativeDB(
                engagement_id=cid,
                section=section_name,
                content=content,
                status="draft",
                metadata_=metadata,
                entity_map=scrub_result.entity_map,
            )
            db.add(narrative)
            await db.flush()

            # Store entity map in EntityMapDB
            entity_map_record = EntityMapDB(
                engagement_id=cid,
                entity_map=scrub_result.entity_map,
                section=section_name,
            )
            db.add(entity_map_record)

            sections.append(NarrativeSection(
                section=section_name,
                content=content,
                status="draft",
                metadata_=metadata,
                requires_input_flags=requires_input_flags,
                unknown_terms=unknown_terms,
            ))
        except Exception as exc:
            logger.error("Failed to generate section '%s': %s", section_name, exc)
            error_content = f"[ERROR: Failed to generate — {exc}]"
            error_metadata = {"error": str(exc)}

            # Persist error sections so they can be edited/approved via PATCH
            await db.execute(
                sa.delete(NarrativeDB).where(
                    NarrativeDB.engagement_id == cid,
                    NarrativeDB.section == section_name,
                )
            )
            narrative = NarrativeDB(
                engagement_id=cid,
                section=section_name,
                content=error_content,
                status="error",
                metadata_=error_metadata,
                entity_map=scrub_result.entity_map,
            )
            db.add(narrative)
            await db.flush()

            sections.append(NarrativeSection(
                section=section_name,
                content=error_content,
                status="error",
                metadata_=error_metadata,
                requires_input_flags=[],
                unknown_terms=[],
            ))

    await db.flush()

    return {
        "engagement_id": str(cid),
        "sections": [s.model_dump() for s in sections],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/engagements/{company_id}/narrative/{section}", tags=["Narrative"])
async def generate_single_section(
    company_id: str,
    section: str,
    req: NarrativeSectionRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Regenerate a single narrative section. If director_notes is omitted,
    reuses previously stored notes (from entity map).
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    if section not in VALID_SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid section name '{section}'. Valid sections: {sorted(VALID_SECTIONS)}",
        )

    # Validate engagement exists
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Get director notes — from request or from stored narrative
    director_notes = req.director_notes
    if not director_notes:
        raise HTTPException(
            status_code=400,
            detail="director_notes is required for section regeneration",
        )

    from services.privacy_vault import scrub, restore

    # Build known entities from DB + request
    known = req.known_entities or {}
    if company.legal_name:
        known.setdefault("counterparty", [])
        if company.legal_name not in known["counterparty"]:
            known["counterparty"].append(company.legal_name)
    if getattr(company, "trading_name", None) and company.trading_name != company.legal_name:
        known.setdefault("counterparty", [])
        if company.trading_name not in known["counterparty"]:
            known["counterparty"].append(company.trading_name)

    scrub_result = scrub(director_notes, known_entities=known if known else None)

    # Load plan parameters
    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan_row = plan_result.scalar_one_or_none()

    engagement_data = {
        "company_name": company.legal_name,
        "acn": company.acn,
        "abn": company.abn,
    }
    if plan_row:
        engagement_data.update({
            "total_contribution": plan_row.total_contribution,
            "practitioner_fee_pct": plan_row.practitioner_fee_pct,
            "num_initial_payments": plan_row.num_initial_payments,
            "initial_payment_amount": plan_row.initial_payment_amount,
            "num_ongoing_payments": plan_row.num_ongoing_payments,
            "ongoing_payment_amount": plan_row.ongoing_payment_amount,
        })

    comparison_data = None
    if plan_row:
        try:
            asset_result = await db.execute(
                sa.select(AssetDB).where(AssetDB.company_id == cid)
            )
            assets_rows = asset_result.scalars().all()
            cred_result = await db.execute(
                sa.select(CreditorDB).where(CreditorDB.company_id == cid)
            )
            creditors = cred_result.scalars().all()
            if assets_rows and creditors:
                assets = [
                    {
                        "asset_type": a.asset_type,
                        "description": a.description,
                        "book_value": a.book_value,
                        "liquidation_recovery_pct": a.liquidation_recovery_pct,
                        "liquidation_value": a.liquidation_value,
                    }
                    for a in assets_rows
                ]
                stored_total = float(company.total_creditors or 0)
                creditors_total = stored_total if stored_total > 0 else sum(float(c.amount_claimed) for c in creditors)
                plan = {
                    "total_contribution": plan_row.total_contribution,
                    "practitioner_fee_pct": plan_row.practitioner_fee_pct,
                    "est_liquidator_fees": plan_row.est_liquidator_fees,
                    "est_legal_fees": plan_row.est_legal_fees,
                    "est_disbursements": plan_row.est_disbursements,
                }
                comparison_data = comparison_engine.calculate(assets, creditors_total, plan)
        except Exception:
            logger.warning("Could not load comparison data for section regeneration")

    try:
        from services.claude_client import ClaudeClient
        from services.narrative_generator import NarrativeGenerator

        claude_client = ClaudeClient()
        custom_terms = req.custom_terms or {}
        if company.custom_glossary:
            merged_terms = {**company.custom_glossary, **custom_terms}
        else:
            merged_terms = custom_terms

        generator = NarrativeGenerator(
            claude_client=claude_client,
            industry=req.industry,
            custom_terms=merged_terms or None,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=f"Claude API unavailable: {exc}")

    # Map section to generator method
    gen_map = {
        "background": lambda: generator.generate_background(scrub_result.scrubbed_text, engagement_data),
        "distress_events": lambda: generator.generate_distress_events(scrub_result.scrubbed_text, engagement_data),
        "expert_advice": lambda: generator.generate_expert_advice(engagement_data),
        "plan_summary": lambda: generator.generate_plan_summary(engagement_data, comparison_data or {}),
        "viability": lambda: generator.generate_viability(scrub_result.scrubbed_text, engagement_data),
        "comparison_commentary": lambda: generator.generate_comparison_commentary(comparison_data or {}),
    }

    try:
        output = await gen_map[section]()
        content = restore(output["content"], scrub_result.entity_map)
        metadata = output["metadata"]
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=f"Claude API unavailable: {exc}")

    # Delete existing section
    await db.execute(
        sa.delete(NarrativeDB).where(
            NarrativeDB.engagement_id == cid,
            NarrativeDB.section == section,
        )
    )

    # Store
    narrative = NarrativeDB(
        engagement_id=cid,
        section=section,
        content=content,
        status="draft",
        metadata_=metadata,
        entity_map=scrub_result.entity_map,
    )
    db.add(narrative)
    await db.flush()

    entity_map_record = EntityMapDB(
        engagement_id=cid,
        entity_map=scrub_result.entity_map,
        section=section,
    )
    db.add(entity_map_record)

    requires_input_flags = metadata.get("requires_input_flags", [])
    unknown_terms = metadata.get("unknown_terms_flagged", [])

    return NarrativeSection(
        section=section,
        content=content,
        status="draft",
        metadata_=metadata,
        requires_input_flags=requires_input_flags,
        unknown_terms=unknown_terms,
    ).model_dump()


@app.patch("/api/engagements/{company_id}/narrative/{section}", tags=["Narrative"])
async def update_narrative_section(
    company_id: str,
    section: str,
    req: NarrativePatchRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Update or approve a narrative section. Supports status transitions:
    draft -> reviewed -> approved, and approved -> draft.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    if section not in VALID_SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid section name '{section}'. Valid sections: {sorted(VALID_SECTIONS)}",
        )

    # Find the narrative section
    result = await db.execute(
        sa.select(NarrativeDB).where(
            NarrativeDB.engagement_id == cid,
            NarrativeDB.section == section,
        )
    )
    narrative = result.scalar_one_or_none()
    if not narrative:
        raise HTTPException(status_code=404, detail=f"Narrative section '{section}' not found for this engagement")

    # Update content if provided
    if req.content is not None:
        narrative.content = req.content

    # Update status if provided
    if req.status is not None:
        valid_statuses = {"draft", "reviewed", "approved"}
        if req.status not in valid_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status '{req.status}'. Valid statuses: {sorted(valid_statuses)}",
            )
        narrative.status = req.status

    await db.flush()

    return NarrativeSection(
        section=narrative.section,
        content=narrative.content,
        status=narrative.status,
        metadata_=narrative.metadata_,
        requires_input_flags=narrative.metadata_.get("requires_input_flags", []) if narrative.metadata_ else [],
        unknown_terms=narrative.metadata_.get("unknown_terms_flagged", []) if narrative.metadata_ else [],
    ).model_dump()


@app.get("/api/engagements/{company_id}/narrative", tags=["Narrative"])
async def get_all_narrative_sections(
    company_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get all narrative sections for an engagement with summary metadata.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Verify engagement exists
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Load all narrative sections
    result = await db.execute(
        sa.select(NarrativeDB).where(NarrativeDB.engagement_id == cid)
    )
    narratives = result.scalars().all()

    sections = []
    total_requires_input = 0
    total_unknown_terms = 0
    for n in narratives:
        ri_flags = n.metadata_.get("requires_input_flags", []) if n.metadata_ else []
        ut_flags = n.metadata_.get("unknown_terms_flagged", []) if n.metadata_ else []
        total_requires_input += len(ri_flags)
        total_unknown_terms += len(ut_flags)
        sections.append(NarrativeSection(
            section=n.section,
            content=n.content,
            status=n.status,
            metadata_=n.metadata_,
            requires_input_flags=ri_flags,
            unknown_terms=ut_flags,
        ).model_dump())

    all_approved = (
        len(narratives) == len(VALID_SECTIONS)
        and all(n.status == "approved" for n in narratives)
    )

    return {
        "engagement_id": str(cid),
        "sections": sections,
        "all_approved": all_approved,
        "requires_input_count": total_requires_input,
        "unknown_terms_count": total_unknown_terms,
    }


@app.get("/api/engagements/{company_id}/narrative/{section}", tags=["Narrative"])
async def get_single_narrative_section(
    company_id: str,
    section: str,
    db: AsyncSession = Depends(get_db),
):
    """Get a single narrative section."""
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    if section not in VALID_SECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid section name '{section}'. Valid sections: {sorted(VALID_SECTIONS)}",
        )

    result = await db.execute(
        sa.select(NarrativeDB).where(
            NarrativeDB.engagement_id == cid,
            NarrativeDB.section == section,
        )
    )
    narrative = result.scalar_one_or_none()
    if not narrative:
        raise HTTPException(status_code=404, detail=f"Narrative section '{section}' not found for this engagement")

    return NarrativeSection(
        section=narrative.section,
        content=narrative.content,
        status=narrative.status,
        metadata_=narrative.metadata_,
        requires_input_flags=narrative.metadata_.get("requires_input_flags", []) if narrative.metadata_ else [],
        unknown_terms=narrative.metadata_.get("unknown_terms_flagged", []) if narrative.metadata_ else [],
    ).model_dump()


# ===================================================================
# Glossary endpoints (2.3)
# ===================================================================


@app.get("/api/glossary/insolvency", tags=["Glossary"])
async def get_insolvency_glossary():
    """Return the Layer 1 insolvency glossary terms."""
    glossary_path = GLOSSARY_DIR / "insolvency_layer1.json"
    if not glossary_path.exists():
        raise HTTPException(status_code=500, detail="Insolvency glossary file not found")

    with open(glossary_path) as f:
        data = json.load(f)

    terms = data.get("terms", {})
    return {
        "layer": "insolvency",
        "term_count": len(terms),
        "terms": terms,
    }


@app.get("/api/glossary/{industry}", tags=["Glossary"])
async def get_industry_glossary(industry: str):
    """Return the Layer 2 industry glossary for a given industry."""
    glossary_path = GLOSSARY_DIR / f"{industry}_layer2.json"
    if not glossary_path.exists():
        # List available industries
        available = []
        for f in GLOSSARY_DIR.glob("*_layer2.json"):
            available.append(f.stem.replace("_layer2", ""))
        raise HTTPException(
            status_code=404,
            detail={"message": f"Industry glossary '{industry}' not found", "available_industries": sorted(available)},
        )

    with open(glossary_path) as f:
        data = json.load(f)

    terms = data.get("terms", {})
    return {
        "layer": industry,
        "term_count": len(terms),
        "terms": terms,
    }


@app.post("/api/engagements/{company_id}/glossary/terms", tags=["Glossary"])
async def add_custom_terms(
    company_id: str,
    req: CustomTermsRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Add client-specific terms (Layer 3) for an engagement.
    Returns the merged glossary (Layer 1 + Layer 2 + Layer 3).
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Verify engagement exists
    result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Merge with existing custom glossary
    existing = company.custom_glossary or {}
    existing.update(req.terms)
    company.custom_glossary = existing
    await db.flush()

    # Build merged glossary (Layer 1 + Layer 2 + Layer 3)
    merged: dict[str, str] = {}

    # Layer 1
    layer1_path = GLOSSARY_DIR / "insolvency_layer1.json"
    if layer1_path.exists():
        with open(layer1_path) as f:
            data = json.load(f)
        merged.update(data.get("terms", {}))

    # Layer 2 — check all available
    for f in GLOSSARY_DIR.glob("*_layer2.json"):
        with open(f) as fh:
            data = json.load(fh)
        merged.update(data.get("terms", {}))

    # Layer 3 — custom terms
    merged.update(existing)

    return {
        "engagement_id": str(cid),
        "custom_terms": existing,
        "merged_glossary": merged,
        "total_terms": len(merged),
    }


# ===================================================================
# Document Download + Generation endpoints (3.2)
# ===================================================================


async def _track_document_output(
    db: AsyncSession,
    engagement_id,
    document_type: str,
    filename: str,
    metadata: dict | None = None,
) -> DocumentOutputDB:
    """Track a generated document in DocumentOutputDB with auto-incrementing version."""
    import uuid as _uuid

    # Find max existing version for this engagement + document type
    result = await db.execute(
        sa.select(sa.func.max(DocumentOutputDB.version)).where(
            DocumentOutputDB.engagement_id == engagement_id,
            DocumentOutputDB.document_type == document_type,
        )
    )
    max_version = result.scalar_one_or_none() or 0
    new_version = max_version + 1

    record = DocumentOutputDB(
        engagement_id=engagement_id,
        document_type=document_type,
        version=new_version,
        filename=filename,
        metadata_=metadata,
    )
    db.add(record)
    await db.flush()
    return record


def _safe_company_name(name: str) -> str:
    """Sanitize company name for use in filenames: replace spaces with underscores, strip special chars."""
    import re
    safe = re.sub(r"[^\w\s-]", "", name)
    safe = re.sub(r"\s+", "_", safe.strip())
    return safe[:40]


def _au_date_str(d: date | None = None) -> str:
    """Format date as DDMMYYYY for Australian-style filenames."""
    d = d or date.today()
    return d.strftime("%d%m%Y")


@app.post(
    "/api/engagements/{company_id}/generate/comparison",
    tags=["Document Generation"],
    summary="Generate Annexure G comparison .docx",
)
async def generate_comparison_download(
    company_id: str, db: AsyncSession = Depends(get_db)
):
    """
    Generate the Annexure G — Comparison of Estimated Return to Creditors
    document and return as a downloadable .docx file.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # 1. Validate engagement exists
    company_result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # 2. Load comparison data (need assets, creditors, plan params)
    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan_row = plan_result.scalar_one_or_none()
    if not plan_row:
        raise HTTPException(status_code=400, detail="Set plan parameters first")

    cred_result = await db.execute(
        sa.select(CreditorDB).where(CreditorDB.company_id == cid)
    )
    creditors = cred_result.scalars().all()
    if not creditors:
        raise HTTPException(status_code=400, detail="Run comparison first")

    asset_result = await db.execute(
        sa.select(AssetDB).where(AssetDB.company_id == cid)
    )
    assets_rows = asset_result.scalars().all()
    if not assets_rows:
        raise HTTPException(status_code=400, detail="Run comparison first")

    # Build dicts for comparison engine
    assets = [
        {
            "asset_type": a.asset_type,
            "description": a.description,
            "book_value": a.book_value,
            "liquidation_recovery_pct": a.liquidation_recovery_pct,
            "liquidation_value": a.liquidation_value,
        }
        for a in assets_rows
    ]
    stored_total = float(company.total_creditors or 0)
    creditors_total = stored_total if stored_total > 0 else sum(float(c.amount_claimed) for c in creditors)

    plan = {
        "total_contribution": plan_row.total_contribution,
        "practitioner_fee_pct": plan_row.practitioner_fee_pct,
        "est_liquidator_fees": plan_row.est_liquidator_fees,
        "est_legal_fees": plan_row.est_legal_fees,
        "est_disbursements": plan_row.est_disbursements,
    }

    comparison_data = comparison_engine.calculate(assets, creditors_total, plan)

    # 3. Generate .docx via document generator
    filepath = document_generator.generate_comparison_docx(
        comparison_data=comparison_data,
        company_name=company.legal_name,
        acn=company.acn,
    )

    # 4. Build canonical filename
    safe_name = _safe_company_name(company.legal_name)
    date_str = _au_date_str()
    canonical_filename = f"{safe_name}_Annexure_G_Comparison_{date_str}.docx"

    # 5. Track in DocumentOutputDB
    import hashlib
    data_hash = hashlib.sha256(json.dumps(comparison_data, sort_keys=True, default=str).encode()).hexdigest()[:16]
    await _track_document_output(
        db, cid, "comparison", canonical_filename,
        metadata={"data_hash": data_hash},
    )
    await db.commit()

    # 6. Return as FileResponse
    return FileResponse(
        path=str(filepath),
        filename=canonical_filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.post(
    "/api/engagements/{company_id}/generate/payment-schedule",
    tags=["Document Generation"],
    summary="Generate payment schedule .docx",
)
async def generate_payment_schedule_download(
    company_id: str, db: AsyncSession = Depends(get_db)
):
    """
    Generate the Payment Schedule document and return as a downloadable .docx file.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # 1. Validate engagement exists
    company_result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # 2. Load plan parameters
    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan_row = plan_result.scalar_one_or_none()
    if not plan_row:
        raise HTTPException(status_code=400, detail="Set plan parameters first")

    # 3. Generate payment schedule data
    plan = {
        "total_contribution": plan_row.total_contribution,
        "practitioner_fee_pct": plan_row.practitioner_fee_pct,
        "num_initial_payments": plan_row.num_initial_payments,
        "initial_payment_amount": plan_row.initial_payment_amount,
        "num_ongoing_payments": plan_row.num_ongoing_payments,
        "ongoing_payment_amount": plan_row.ongoing_payment_amount,
    }

    try:
        schedule_data = payment_schedule_generator.generate(plan)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # 4. Generate .docx
    filepath = document_generator.generate_payment_schedule_docx(
        schedule_data=schedule_data,
        company_name=company.legal_name,
    )

    # 5. Build canonical filename
    safe_name = _safe_company_name(company.legal_name)
    date_str = _au_date_str()
    canonical_filename = f"{safe_name}_Payment_Schedule_{date_str}.docx"

    # 6. Track in DocumentOutputDB
    import hashlib
    data_hash = hashlib.sha256(json.dumps(schedule_data, sort_keys=True, default=str).encode()).hexdigest()[:16]
    await _track_document_output(
        db, cid, "payment_schedule", canonical_filename,
        metadata={"data_hash": data_hash},
    )
    await db.commit()

    # 7. Return as FileResponse
    return FileResponse(
        path=str(filepath),
        filename=canonical_filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.post(
    "/api/engagements/{company_id}/generate/company-statement",
    tags=["Document Generation"],
    summary="Generate Company Offer Statement .docx",
)
async def generate_company_statement_download(
    company_id: str, db: AsyncSession = Depends(get_db)
):
    """
    Generate the Company Offer Statement from narrative sections
    and return as a downloadable .docx file.

    Includes X-Draft-Sections header if any sections are unapproved.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # 1. Validate engagement exists
    company_result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # 2. Load narrative sections
    narrative_result = await db.execute(
        sa.select(NarrativeDB).where(NarrativeDB.engagement_id == cid)
    )
    narratives = narrative_result.scalars().all()
    if not narratives:
        raise HTTPException(status_code=400, detail="Generate narratives first")

    # 3. Convert to dicts for generator
    sections = [
        {
            "section": n.section,
            "content": n.content,
            "status": n.status,
        }
        for n in narratives
    ]

    # Check for unapproved sections
    draft_sections = [n.section for n in narratives if n.status != "approved"]

    # 4. Generate .docx
    filepath = document_generator.generate_company_statement_docx(
        sections=sections,
        company_name=company.legal_name,
        acn=company.acn,
    )

    # 5. Build canonical filename
    safe_name = _safe_company_name(company.legal_name)
    date_str = _au_date_str()
    canonical_filename = f"{safe_name}_Company_Offer_Statement_{date_str}.docx"

    # 6. Track in DocumentOutputDB
    section_statuses = {n.section: n.status for n in narratives}
    await _track_document_output(
        db, cid, "company_statement", canonical_filename,
        metadata={"section_statuses": section_statuses},
    )
    await db.commit()

    # 7. Return as FileResponse with draft header if applicable
    from starlette.responses import FileResponse as StarletteFileResponse

    response = FileResponse(
        path=str(filepath),
        filename=canonical_filename,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    if draft_sections:
        response.headers["X-Draft-Sections"] = ",".join(draft_sections)

    return response


@app.post(
    "/api/engagements/{company_id}/generate/all",
    tags=["Document Generation"],
    summary="Generate all SBR documents as ZIP",
)
async def generate_all_documents_download(
    company_id: str, db: AsyncSession = Depends(get_db)
):
    """
    Generate all three SBR documents (comparison, payment schedule,
    company statement) and return as a ZIP archive.
    """
    import uuid as _uuid
    import zipfile

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # 1. Validate engagement exists
    company_result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    safe_name = _safe_company_name(company.legal_name)
    date_str = _au_date_str()

    # 2. Generate comparison document
    plan_result = await db.execute(
        sa.select(PlanParametersDB).where(PlanParametersDB.company_id == cid)
    )
    plan_row = plan_result.scalar_one_or_none()
    if not plan_row:
        raise HTTPException(status_code=400, detail="Set plan parameters first")

    cred_result = await db.execute(
        sa.select(CreditorDB).where(CreditorDB.company_id == cid)
    )
    creditors = cred_result.scalars().all()
    if not creditors:
        raise HTTPException(status_code=400, detail="Run comparison first")

    asset_result = await db.execute(
        sa.select(AssetDB).where(AssetDB.company_id == cid)
    )
    assets_rows = asset_result.scalars().all()
    if not assets_rows:
        raise HTTPException(status_code=400, detail="Run comparison first")

    assets = [
        {
            "asset_type": a.asset_type,
            "description": a.description,
            "book_value": a.book_value,
            "liquidation_recovery_pct": a.liquidation_recovery_pct,
            "liquidation_value": a.liquidation_value,
        }
        for a in assets_rows
    ]
    stored_total = float(company.total_creditors or 0)
    creditors_total = stored_total if stored_total > 0 else sum(float(c.amount_claimed) for c in creditors)

    plan = {
        "total_contribution": plan_row.total_contribution,
        "practitioner_fee_pct": plan_row.practitioner_fee_pct,
        "est_liquidator_fees": plan_row.est_liquidator_fees,
        "est_legal_fees": plan_row.est_legal_fees,
        "est_disbursements": plan_row.est_disbursements,
    }

    comparison_data = comparison_engine.calculate(assets, creditors_total, plan)
    comparison_filepath = document_generator.generate_comparison_docx(
        comparison_data=comparison_data,
        company_name=company.legal_name,
        acn=company.acn,
    )
    comparison_filename = f"{safe_name}_Annexure_G_Comparison_{date_str}.docx"

    # 3. Generate payment schedule document
    schedule_plan = {
        "total_contribution": plan_row.total_contribution,
        "practitioner_fee_pct": plan_row.practitioner_fee_pct,
        "num_initial_payments": plan_row.num_initial_payments,
        "initial_payment_amount": plan_row.initial_payment_amount,
        "num_ongoing_payments": plan_row.num_ongoing_payments,
        "ongoing_payment_amount": plan_row.ongoing_payment_amount,
    }

    try:
        schedule_data = payment_schedule_generator.generate(schedule_plan)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    payment_filepath = document_generator.generate_payment_schedule_docx(
        schedule_data=schedule_data,
        company_name=company.legal_name,
    )
    payment_filename = f"{safe_name}_Payment_Schedule_{date_str}.docx"

    # 4. Generate company statement document (optional — skip if no narratives yet)
    narrative_result = await db.execute(
        sa.select(NarrativeDB).where(NarrativeDB.engagement_id == cid)
    )
    narratives = narrative_result.scalars().all()

    statement_filepath = None
    statement_filename = None
    if narratives:
        sections = [
            {
                "section": n.section,
                "content": n.content,
                "status": n.status,
            }
            for n in narratives
        ]

        statement_filepath = document_generator.generate_company_statement_docx(
            sections=sections,
            company_name=company.legal_name,
            acn=company.acn,
        )
        statement_filename = f"{safe_name}_Company_Offer_Statement_{date_str}.docx"

    # 5. Bundle into ZIP
    tmp_dir = tempfile.mkdtemp()
    zip_filename = f"{safe_name}_SBR_Documents_{date_str}.zip"
    zip_path = os.path.join(tmp_dir, zip_filename)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(str(comparison_filepath), comparison_filename)
        zf.write(str(payment_filepath), payment_filename)
        if statement_filepath:
            zf.write(str(statement_filepath), statement_filename)

    # 6. Track documents in DocumentOutputDB
    await _track_document_output(db, cid, "comparison", comparison_filename)
    await _track_document_output(db, cid, "payment_schedule", payment_filename)
    if narratives and statement_filename:
        section_statuses = {n.section: n.status for n in narratives}
        await _track_document_output(
            db, cid, "company_statement", statement_filename,
            metadata={"section_statuses": section_statuses},
        )
    await db.commit()

    # 7. Return ZIP as FileResponse
    return FileResponse(
        path=zip_path,
        filename=zip_filename,
        media_type="application/zip",
    )


@app.get(
    "/api/engagements/{company_id}/documents",
    response_model=DocumentOutputListResponse,
    tags=["Document Generation"],
    summary="List generated documents for an engagement",
)
async def list_engagement_documents(
    company_id: str, db: AsyncSession = Depends(get_db)
):
    """
    List all generated documents for an engagement, including version history.
    """
    import uuid as _uuid

    try:
        cid = _uuid.UUID(company_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company ID format")

    # Validate engagement exists
    company_result = await db.execute(
        sa.select(CompanyDB).where(CompanyDB.id == cid)
    )
    company = company_result.scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Engagement not found")

    # Load all document outputs
    doc_result = await db.execute(
        sa.select(DocumentOutputDB)
        .where(DocumentOutputDB.engagement_id == cid)
        .order_by(DocumentOutputDB.generated_at.desc())
    )
    docs = doc_result.scalars().all()

    return DocumentOutputListResponse(
        engagement_id=str(cid),
        documents=[
            DocumentOutputEntry(
                id=str(d.id),
                document_type=d.document_type,
                version=d.version,
                filename=d.filename,
                generated_at=d.generated_at,
                metadata_=d.metadata_,
            )
            for d in docs
        ],
    )


# Serve generated documents
app.mount("/documents", StaticFiles(directory="generated_documents"), name="documents")


# ===================================================================
# Static files & index (preserved from original — must be last)
# ===================================================================

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def read_index():
    return FileResponse("static/index.html")


# ===================================================================
# Entry-point
# ===================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
