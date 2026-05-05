# FidelityBridge API 📈

**Real-time financial data from SEC EDGAR filings.** Access company filings, financial reports, and corporate data for 35+ well-known companies (AAPL, MSFT, GOOG, AMZN, TSLA, NVDA, META, and more) plus thousands more via the SEC EDGAR database.

> **💰 $35K MRR target** — Deployed on Railway, distributed via RapidAPI Marketplace.

---

## 🚀 Quick Start

### 1️⃣ Get an API Key

Subscribe on [RapidAPI](https://rapidapi.com/fidelitybridge/api/fidelitybridge) or use a test key for development:

| Tier | Price | Rate Limit | Test Key |
|------|-------|------------|----------|
| Free | $0/mo | 100 req/day | `test_free_key_001` |
| Basic | $9.99/mo | 5,000 req/day | `test_basic_key_001` |
| Pro | $49.99/mo | 50,000 req/day | `test_pro_key_001` |
| Enterprise | $499.99/mo | Unlimited | `test_enterprise_key_001` |

### 2️⃣ Make Your First Request

```bash
curl -H 'x-api-key: test_free_key_001' \
  https://fidelitybridge-api.railway.app/v1/health
```

### 3️⃣ Search Apple's 10-K Filings

```bash
curl -H 'x-api-key: YOUR_API_KEY' \
  'https://fidelitybridge-api.railway.app/v1/filings?ticker=AAPL&filing_type=10-K&limit=5'
```

---

## 📋 Endpoints

### 🔍 Company Data

#### `GET /v1/company/{cik}`
Get detailed company information by CIK number.

```bash
curl -H 'x-api-key: YOUR_API_KEY' \
  https://fidelitybridge-api.railway.app/v1/company/0000320193
```

**Response:**
```json
{
  "cik": "0000320193",
  "name": "Apple Inc.",
  "ticker": "AAPL",
  "exchange": "NASDAQ",
  "sector": "Technology",
  "industry": "Consumer Electronics",
  "description": "Apple Inc. designs, manufactures, and markets smartphones, ...",
  "fiscal_year_end": "September 30"
}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `cik` | string | SEC CIK number (padded to 10 digits) |

#### `GET /v1/companies`
List all 35+ curated companies with their CIK numbers and tickers.

```bash
curl -H 'x-api-key: YOUR_API_KEY' \
  https://fidelitybridge-api.railway.app/v1/companies
```

---

### 📄 SEC Filings

#### `GET /v1/filings`
Search SEC filings by company CIK or ticker symbol.

```bash
curl -H 'x-api-key: YOUR_API_KEY' \
  'https://fidelitybridge-api.railway.app/v1/filings?ticker=MSFT&filing_type=10-K&limit=3'
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cik` | string | No* | SEC CIK number |
| `ticker` | string | No* | Stock ticker symbol |
| `filing_type` | string | No | Filter by type (10-K, 10-Q, 8-K, etc.) |
| `limit` | int | No | Results per page (1-100, default: 10) |
| `offset` | int | No | Pagination offset (default: 0) |

*Either `cik` or `ticker` is required.

**Response:**
```json
{
  "results": [
    {
      "accession": "0000789019-24-000099",
      "filing_type": "10-K",
      "filing_date": "2024-07-30",
      "period_end": "2024-06-30",
      "description": "Annual Report for fiscal year ended June 30, 2024",
      "url": "https://www.sec.gov/Archives/edgar/data/789019/..."
    }
  ],
  "total": 1,
  "limit": 3,
  "offset": 0,
  "company_cik": "0000789019",
  "company_name": "Microsoft Corporation"
}
```

#### `GET /v1/filing/{accession}`
Get detailed information about a specific filing.

```bash
curl -H 'x-api-key: YOUR_API_KEY' \
  https://fidelitybridge-api.railway.app/v1/filing/0000320193-24-000123
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `accession` | string | Filing accession number with hyphens |

---

### 💚 Health

#### `GET /v1/health`
Quick health check (no API key required, but rate limited).

```bash
curl https://fidelitybridge-api.railway.app/v1/health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2025-05-05T12:00:00Z",
  "companies_in_curated_dataset": 35
}
```

---

## 🔐 Authentication

All endpoints (except `/v1/health`) require an API key. Include it in the header:

| Header | Value |
|--------|-------|
| `x-api-key` | `your_api_key_here` |

On RapidAPI, the marketplace automatically adds `x-rapidapi-key` and `x-rapidapi-host` headers. Both are supported.

---

## 📊 Rate Limits

| Tier | Daily Limit | Per-Minute | Burst |
|------|-------------|------------|-------|
| Free | 100 | 30/min | n/a |
| Basic | 5,000 | 60/min | n/a |
| Pro | 50,000 | 120/min | n/a |
| Enterprise | 1,000,000 | 300/min | n/a |

Rate limit headers are returned with every response:
- `X-RateLimit-Limit`
- `X-RateLimit-Remaining`
- `X-RateLimit-Reset`

---

## 🐍 SDK Examples

### Python
```python
import requests

headers = {"x-api-key": "YOUR_API_KEY"}

# Get company data
resp = requests.get(
    "https://fidelitybridge-api.railway.app/v1/company/0000320193",
    headers=headers
)
company = resp.json()
print(f"Company: {company['name']}")

# Search filings
resp = requests.get(
    "https://fidelitybridge-api.railway.app/v1/filings",
    params={"ticker": "AAPL", "filing_type": "10-K"},
    headers=headers
)
filings = resp.json()
for f in filings["results"]:
    print(f"{f['filing_type']} - {f['filing_date']}")
```

### JavaScript
```javascript
const headers = { 'x-api-key': 'YOUR_API_KEY' };

// Get company data
const res = await fetch(
  'https://fidelitybridge-api.railway.app/v1/company/0000320193',
  { headers }
);
const company = await res.json();
console.log(`Company: ${company.name}`);
```

### cURL
```bash
# TIER: Test the API
curl -H 'x-api-key: YOUR_API_KEY' \
  https://fidelitybridge-api.railway.app/v1/health

# COMPANY: Get NVIDIA data
curl -H 'x-api-key: YOUR_API_KEY' \
  https://fidelitybridge-api.railway.app/v1/company/0001045810

# FILINGS: Search MSFT 10-Q filings
curl -H 'x-api-key: YOUR_API_KEY' \
  'https://fidelitybridge-api.railway.app/v1/filings?ticker=MSFT&filing_type=10-Q&limit=3'

# FILING: Get specific filing details
curl -H 'x-api-key: YOUR_API_KEY' \
  https://fidelitybridge-api.railway.app/v1/filing/0000320193-24-000123
```

---

## 🏗️ Architecture

```
┌──────────────┐     ┌──────────────┐     ┌────────────┐
│   RapidAPI   │────▶│  Railway CDN │────▶│  Railway   │
│  Marketplace │     │  Edge Cache  │     │  App       │
└──────────────┘     └──────────────┘     └─────┬──────┘
                                                │
                          ┌─────────────────────┴──────┐
                          │         FastAPI App         │
                          │  ┌──────────────────────┐   │
                          │  │   API Key Auth       │   │
                          │  │   Rate Limiting      │   │
                          │  │   CORS Middleware    │   │
                          │  │   Data Pipeline      │   │
                          │  └──────────────────────┘   │
                          └─────────────────────────────┘
                                                │
                          ┌─────────────────────┴──────┐
                          │   SEC EDGAR (data.sec.gov) │
                          │   + 15-min TTL Cache       │
                          │   + 35 Company Dataset     │
                          └────────────────────────────┘
```

### Data Flow
1. **Request** hits FastAPI via Railway
2. **Auth** middleware validates `x-api-key`
3. **Rate limiter** checks tier allowance
4. **Data pipeline** checks 15-min TTL cache
5. **Cache miss?** Fetches from SEC EDGAR with proper User-Agent
6. **Response** returned with rate limit headers

---

## 🚢 Deployment

### Deploy to Railway

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/fidelitybridge)

1. Fork this repo
2. Connect to Railway
3. Set env vars:
   - `FIDELITY_API_KEYS` — Comma-separated: `key1:tier1,key2:tier2`
   - `ENV` — `production` (default)
4. Deploy!

### Local Development

```bash
# Clone
git clone https://github.com/51ghost/fidelitybridge-api.git
cd fidelitybridge-api

# Install
pip install -r requirements.txt

# Run (development mode with auto-reload)
ENV=development python main.py

# Or use uvicorn directly
uvicorn main:app --reload --port 8000
```

Visit `http://localhost:8000/docs` for interactive Swagger docs.

### Testing

```bash
python -c "from main import app; print('App loaded OK:', app.title)"
```

---

## 📦 Curated Companies Dataset

The API ships with a built-in dataset of **35+ well-known companies** with real CIK numbers and sample filing data. This ensures zero-latency responses for the most-requested tickers:

AAPL, MSFT, GOOGL, GOOG, AMZN, META, BRK.A, BRK.B, JPM, V, NVDA, TSLA, JNJ, WMT, PG, MA, UNH, HD, DIS, NFLX, ADBE, CRM, INTC, AMD, PYPL, BAC, KO, PEP, TMO, COST, ABNB, UBER, SQ, SNAP

The pipeline also supports **any SEC-registered company** via real-time EDGAR lookups with 15-minute caching.

---

## 🧪 Test Keys (Development Only)

| Key | Tier | Purpose |
|-----|------|---------|
| `test_free_key_001` | Free | Dev/testing |
| `test_basic_key_001` | Basic | Dev/testing |
| `test_pro_key_001` | Pro | Dev/testing |
| `test_enterprise_key_001` | Enterprise | Dev/testing |

> **⚠️** In production, set `FIDELITY_API_KEYS` env var and remove test keys.

---

## 📄 License

MIT License — see [LICENSE](LICENSE).

---

## 🤝 Support

- **RapidAPI:** [https://rapidapi.com/fidelitybridge/api/fidelitybridge](https://rapidapi.com/fidelitybridge/api/fidelitybridge)
- **Email:** support@fidelitybridge.com
- **Docs:** https://docs.fidelitybridge.com
- **GitHub:** https://github.com/51ghost/fidelitybridge-api
