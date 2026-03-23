# GR/EPANT - Hellenic Competition Commission

## Overview

Data fetcher for the Hellenic Competition Commission (Επιτροπή Ανταγωνισμού / EPANT).
Fetches competition law decisions including cartel cases, merger clearances,
abuse of dominance decisions, and commitment decisions.

## Data Source

- **Website**: https://www.epant.gr
- **Decisions page**: https://www.epant.gr/en/decisions.html
- **Data type**: case_law
- **Language**: English (summaries), Greek (full PDFs)
- **Coverage**: 1990s to present (~400+ decisions)
- **Update frequency**: Weekly/Monthly

## Data Retrieved

Each decision includes:
- Full text summary in English (from JSON-LD articleBody)
- Decision number and year
- Decision type (Decision or Act)
- Date of issuance
- Relevant market
- Companies concerned
- Legal framework (Law 3959/2011, EU Articles 101/102)
- Government Gazette reference
- PDF link (Greek full text)

## Technical Details

- **Pagination**: 23 items per page, uses `?start=N` parameter
- **Full text source**: JSON-LD structured data contains articleBody
- **Rate limit**: 1 request/second
- **Auth**: None required (open public access)

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Legal Framework

Decisions primarily concern:
- Greek Law 3959/2011 (Protection of Free Competition)
- EU Article 101 TFEU (anti-competitive agreements)
- EU Article 102 TFEU (abuse of dominant position)
- Merger control regulations
