"""
FidelityBridge API - Main Application
Financial data from SEC EDGAR filings.
FastAPI application with rate limiting, API key auth, and CORS for RapidAPI.
"""

import os
import time
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Request, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from data_pipeline import (
    CURATED_COMPANIES,
    get_company_data,
    search_filings,
    get_filing_details,
    health_check,
)

# ─── App Setup ──────────────────────────────────────────────────────────────

app = FastAPI(
    title="FidelityBridge API",
    description="Real-time financial data from SEC EDGAR filings. "
                "Access company filings, financial reports, and corporate data "
                "for thousands of publicly traded companies.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "FidelityBridge Support",
        "email": "support@fidelitybridge.com",
        "url": "https://fidelitybridge.com",
    },
)

# ─── Rate Limiting ──────────────────────────────────────────────────────────

limiter = Limiter(key_func=get_remote_address, default_limits=["100/day"])
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# ─── CORS for RapidAPI ─────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://rapidapi.com",
        "https://www.rapidapi.com",
        "https://*.rapidapi.com",
        "https://fidelitybridge.com",
        "https://www.fidelitybridge.com",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=[
        "x-api-key",
        "X-API-Key",
        "x-rapidapi-key",
        "X-RapidAPI-Key",
        "x-rapidapi-host",
        "Content-Type",
        "Authorization",
    ],
    expose_headers=[
        "X-RateLimit-Limit",
        "X-RateLimit-Remaining",
        "X-RateLimit-Reset",
    ],
)

# ─── API Key Authentication ────────────────────────────────────────────────

# In production, load from env vars or a database
API_KEYS = {}

def load_api_keys():
    """Load API keys from environment or use defaults."""
    env_keys = os.environ.get("FIDELITY_API_KEYS", "")
    if env_keys:
        for pair in env_keys.split(","):
            if ":" in pair:
                key, tier = pair.split(":", 1)
                API_KEYS[key.strip()] = tier.strip()
    else:
        # Development/testing defaults
        API_KEYS["test_free_key_001"] = "free"
        API_KEYS["test_basic_key_001"] = "basic"
        API_KEYS["test_pro_key_001"] = "pro"
        API_KEYS["test_enterprise_key_001"] = "enterprise"

load_api_keys()

# Rate limit configuration per tier (per day)
TIER_LIMITS = {
    "free": 100,
    "basic": 5000,
    "pro": 50000,
    "enterprise": 1000000,  # effectively unlimited
}

async def verify_api_key(x_api_key: Optional[str] = Header(None)) -> str:
    """Verify the x-api-key header and return the tier."""
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide it via x-api-key header.",
            headers={"WWW-Authenticate": "API-Key"},
        )
    tier = API_KEYS.get(x_api_key)
    if not tier:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key. Check your key or visit https://rapidapi.com/fidelitybridge.",
        )
    return tier

# ─── Request Models ─────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str
    database: str
    cache_ttl_seconds: int
    companies_in_curated_dataset: int

class CompanyResponse(BaseModel):
    cik: str
    name: str
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    description: Optional[str] = None
    fiscal_year_end: Optional[str] = None

class FilingResult(BaseModel):
    accession: str
    filing_type: str
    filing_date: str
    period_end: str
    description: str
    url: str

class FilingSearchResponse(BaseModel):
    results: list[FilingResult]
    total: int
    limit: int
    offset: int
    company_cik: Optional[str] = None
    company_name: Optional[str] = None

class FilingDetailResponse(BaseModel):
    accession: str
    filing_type: str
    filing_date: str
    period_end: Optional[str] = None
    description: str
    url: Optional[str] = None
    sec_url: Optional[str] = None
    filing_text_snippet: Optional[str] = None

# ─── Endpoints ──────────────────────────────────────────────────────────────

@app.get(
    "/v1/health",
    response_model=HealthResponse,
    tags=["Health"],
    summary="API Health Check",
    description="Check if the API is operational and get status information.",
)
@limiter.limit("60/minute")
async def v1_health(request: Request):
    """Health check endpoint with rate limiting."""
    return await health_check()


@app.get(
    "/v1/company/{cik}",
    response_model=CompanyResponse,
    tags=["Company"],
    summary="Get Company Data",
    description="Retrieve detailed company information by CIK (Central Index Key) number.",
)
@limiter.limit("30/minute")
async def v1_company(
    request: Request,
    cik: str,
    tier: str = Depends(verify_api_key),
):
    """Get company details by CIK."""
    company = await get_company_data(cik)
    if not company:
        raise HTTPException(
            status_code=404,
            detail=f"Company with CIK '{cik}' not found.",
        )
    return company


@app.get(
    "/v1/filings",
    response_model=FilingSearchResponse,
    tags=["Filings"],
    summary="Search SEC Filings",
    description="Search SEC EDGAR filings by CIK, ticker symbol, and filing type. "
                "Supports pagination with limit and offset parameters.",
)
@limiter.limit("30/minute")
async def v1_filings(
    request: Request,
    cik: Optional[str] = Query(None, description="SEC CIK number (e.g., 320193 for Apple)"),
    ticker: Optional[str] = Query(None, description="Ticker symbol (e.g., AAPL)"),
    filing_type: Optional[str] = Query(None, description="Filing type filter (e.g., 10-K, 10-Q, 8-K)"),
    limit: int = Query(10, ge=1, le=100, description="Number of results to return (max 100)"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    tier: str = Depends(verify_api_key),
):
    """Search SEC filings with filters."""
    if not cik and not ticker:
        raise HTTPException(
            status_code=400,
            detail="Either 'cik' or 'ticker' parameter is required.",
        )
    result = await search_filings(
        cik=cik,
        ticker=ticker,
        filing_type=filing_type,
        limit=limit,
        offset=offset,
    )
    return result


@app.get(
    "/v1/filing/{accession}",
    response_model=FilingDetailResponse,
    tags=["Filings"],
    summary="Get Filing Details",
    description="Retrieve details for a specific SEC filing by its accession number.",
)
@limiter.limit("30/minute")
async def v1_filing_detail(
    request: Request,
    accession: str,
    tier: str = Depends(verify_api_key),
):
    """Get details of a specific filing by accession number."""
    detail = await get_filing_details(accession)
    if not detail:
        raise HTTPException(
            status_code=404,
            detail=f"Filing with accession number '{accession}' not found.",
        )
    return detail


@app.get(
    "/v1/companies",
    tags=["Company"],
    summary="List Curated Companies",
    description="Get a list of all companies in the curated dataset with their CIK numbers and tickers.",
)
@limiter.limit("30/minute")
async def v1_companies(
    request: Request,
    tier: str = Depends(verify_api_key),
):
    """List all curated companies."""
    companies = []
    seen_ciks = set()
    for ticker, info in CURATED_COMPANIES.items():
        if info["cik"] not in seen_ciks:
            companies.append({
                "ticker": info.get("ticker", ticker),
                "cik": info["cik"],
                "name": info["name"],
                "exchange": info.get("exchange"),
                "sector": info.get("sector"),
            })
            seen_ciks.add(info["cik"])
    return {"companies": companies, "total": len(companies)}


# ─── Error Handling ─────────────────────────────────────────────────────────

@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "code": exc.status_code,
            "message": exc.detail,
            "documentation": "https://docs.fidelitybridge.com",
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "code": 500,
            "message": "Internal server error. Please try again later.",
            "documentation": "https://docs.fidelitybridge.com",
        },
    )


# ─── Startup ────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    """Initialize the app on startup."""
    load_api_keys()
    print(f"FidelityBridge API v1.0.0 starting...")
    print(f"Curated dataset: {len(CURATED_COMPANIES)} tickers loaded")
    print(f"API keys loaded: {len(API_KEYS)}")

# ─── Entry Point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=os.environ.get("ENV", "production") == "development",
    )
