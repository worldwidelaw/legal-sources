# FI/Finlex -- Finnish Legal Database

## Overview

[Finlex](https://www.finlex.fi) is Finland's official legal database, operated by the Ministry of Justice. It provides comprehensive access to Finnish legislation, treaties, government proposals, and authority regulations.

## Data Access

**API Endpoint:** `https://opendata.finlex.fi/finlex/avoindata/v1`

**Authentication:** None required (User-Agent header mandatory)

**Format:** Akoma Ntoso XML (international legal document standard)

## Available Data Types

| Type | Endpoint | Description |
|------|----------|-------------|
| `statute` | `/akn/fi/act/statute/list` | Original statutes as published |
| `statute-consolidated` | `/akn/fi/act/statute-consolidated/list` | Up-to-date consolidated law |
| `treaty` | `/akn/fi/doc/treaty/list` | International treaties |
| `government-proposal` | `/akn/fi/doc/government-proposal/list` | Government bills |

**Note:** Case law (Supreme Court/KKO, Supreme Administrative Court/KHO) requires authentication and is not included in this implementation.

## Languages

- Finnish (fin) - primary
- Swedish (swe) - official translation
- Sámi languages (sme, smn, sms) - minority language translations
- English (eng) - unofficial translations of major acts

## License

- Original statutes: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Translations: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Consolidated legislation: [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)

> ⚠️ **Consolidated legislation restricts commercial use** (CC BY-NC 4.0). Original statutes and translations are freely reusable.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (10 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (large dataset; resumes from its checkpoint if relaunched)
python bootstrap.py bootstrap
python bootstrap.py bootstrap-fast   # alias the fleet wrapper calls

# Incremental update
python bootstrap.py update
```

## API Documentation

- [Integration Quick Guide](https://www.finlex.fi/en/open-data/integration-quick-guide)
- [Open Data Portal](https://www.finlex.fi/en/open-data)
- [Swagger UI](https://opendata.finlex.fi/swagger-ui/index.html)

## Technical Notes

- Pagination: `page` and `limit` parameters — **max 10 per page**; a larger
  `limit` is rejected with a non-JSON error body
- Rate limiting: API may return HTTP 429 on excessive requests
- TLS 1.2+ required (no HTTP)
- Documents are in Akoma Ntoso XML format with full structured text
- Some documents (mostly Swedish renderings of recent treaties) carry a
  `<componentRef src="main.pdf"/>` body instead of marked-up sections. The
  text lives in the PDF at `{akn_uri}/main.pdf` and is extracted from there;
  without that step the record would be a ~200-character preface.

### Ordering, and how the refresh works

The list endpoint returns entries in the order the API store last **wrote**
them, not by document date — so the newest arrivals are on the **last** page,
not page 1. As of 2026-08-30, `doc/treaty` page 1 is a 2025 entry while its
last page (833) holds 2026/29-34, and `doc/government-proposal` ends on
2026/135-136. The final pages of `act/statute` are 1917-2022 acts that were
rewritten recently, which is also why a document's own date is useless as a
freshness test.

There is no modification date, no sitemap `lastmod`, no date-range parameter,
and the API Gateway sends neither `ETag` nor `Last-Modified`. So the refresh
compares against position plus a record of what has already been read:
`data/finlex_checkpoint.json` holds every AKN URI seen and its `NEW`/`MODIFIED`
status, and `fetch_updates` walks **backwards** from the last page, stopping
after 20 consecutive pages that contain nothing unrecorded. The last 20 pages
are always re-read, because an edit to an already-`MODIFIED` record moves it to
the tail without changing its status and would otherwise be invisible.
