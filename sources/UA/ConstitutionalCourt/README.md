# UA/ConstitutionalCourt — Constitutional Court of Ukraine

## Overview

This scraper fetches decisions from the **Constitutional Court of Ukraine** 
(Конституційний Суд України) via the Verkhovna Rada Open Data Portal.

**Website:** https://ccu.gov.ua  
**Data API:** https://data.rada.gov.ua  
**Coverage:** 2,500+ decisions (1997–present)

## Data Source

The Constitutional Court is the sole body of constitutional jurisdiction in Ukraine.
Its decisions are published on the official website (ccu.gov.ua) and indexed in
the Verkhovna Rada legislation database (zakon.rada.gov.ua).

This scraper uses the official Open Data API at data.rada.gov.ua, which provides:
- Full list of all Constitutional Court decisions (org_id=79)
- Plain text content for each decision
- Metadata including dates, decision numbers, and types

## Document Types

| Type Code | Ukrainian | English |
|-----------|-----------|---------|
| 22 | Рішення | Decision |
| 30 | Ухвала | Ruling/Order |
| 153 | Окрема думка | Separate opinion |

## API Endpoints

- **List all decisions:** `GET /laws/main/o79.json`
- **Full text:** `GET /laws/show/{nreg}.txt`
- **JSON metadata:** `GET /laws/show/{nreg}.json`
- **View page:** `https://zakon.rada.gov.ua/laws/show/{nreg}`

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all ~2,500 decisions)
python bootstrap.py bootstrap

# Incremental update (recent decisions)
python bootstrap.py update
```

## Rate Limits

- Anonymous access (User-Agent: "OpenData")
- 60 requests per minute
- 100,000 requests per day
- Recommended delay: 1 second between requests

## Schema

Key fields in normalized output:

| Field | Description |
|-------|-------------|
| `_id` | Unique ID (nreg) |
| `title` | Decision title (Ukrainian) |
| `text` | Full text of decision |
| `date` | Decision date (ISO 8601) |
| `decision_number` | Official number (e.g., 1-р/2021) |
| `decision_type` | Type (decision, ruling, separate_opinion) |
| `court` | Court name |
| `url` | Link to official source |

## Reliability

`normalize()` downloads the document text, and `bootstrap-fast` runs it on several
threads. Two things keep that honest:

- Each worker thread gets its own `requests.Session` (sessions are not thread-safe),
  and a single lock enforces the documented 60 req/min across all of them.
- A document whose text cannot be fetched after 5 attempts is **dropped**, not written
  with an empty `text` — a transient outage must never overwrite a stored decision
  with an empty one. Drops are tallied by reason and printed at the end of a run.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Verkhovna Rada Open Data
Portal terms; attribution required, commercial use permitted.
