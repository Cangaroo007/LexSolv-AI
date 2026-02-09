"""
LexSolv AI — FastAPI backend.

Combines the original demo analysis endpoints with the new Xero / MYOB
accounting integration architecture.
"""

from __future__ import annotations

import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from integrations.xero_client import XeroClient, XeroTokenSet
from integrations.myob_client import MYOBClient, MYOBTokenSet
from models.schemas import (
    CompanyData,
    CreditorList,
    ForensicReport,
    PreferencePaymentReport,
    RelatedPartyReport,
    SolvencyScore,
    Transaction,
)
from db.database import async_engine, Base, get_db
from services.forensic_engine import ForensicAnalyzer

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise database and integration clients on startup."""
    global xero_client, myob_client

    # --- Database ---------------------------------------------------------
    # Import all ORM models so Base.metadata knows about them
    import db.models  # noqa: F401

    if async_engine is not None:
        try:
            logger.info("Connecting to PostgreSQL…")
            async with async_engine.begin() as conn:
                # In production, use Alembic migrations instead of create_all.
                # This is a safety net for local development.
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables verified")
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
