# PL/ConstitutionalCourt

**Polish Constitutional Court (Trybunał Konstytucyjny)**

## Overview

This source fetches case law from the Polish Constitutional Tribunal through **two
paths, merged in place**, because neither one covers the whole corpus:

| Path | Host | Period | Volume |
|------|------|--------|--------|
| IPO (Internetowy Portal Orzeczeń) | `ipo.trybunal.gov.pl` | 2016-01-01 → present | ~1,020 rulings |
| SAOS (System Analizy Orzeczeń Sądowych) | `www.saos.org.pl` | 1985 → 2015-12-09 | 9,503 judgments |

SAOS stopped ingesting Constitutional Tribunal judgments on **2015-12-09**, so on its
own it misses a full decade — including the entire rule-of-law/judicial-independence
period (issue #1469). The Tribunal's own portal fills that decade.

IPO is walked first, so the material SAOS lacks lands before the historical backfill.
The two paths are reconciled on case number (*sygnatura*): a case already yielded by
IPO is not yielded again by SAOS. In practice their date ranges are disjoint.

## Data Source

- **Official Website**: https://trybunal.gov.pl
- **Official rulings portal**: https://ipo.trybunal.gov.pl/ipo/
- **API Provider**: SAOS (https://www.saos.org.pl)
- **API Documentation**: https://www.saos.org.pl/help/index.php/dokumentacja-api

## Coverage

- Constitutional Tribunal rulings from 1985 to the present
- ~10,500 rulings across both paths
- Includes:
  - Wyroki (Judgments/Sentences)
  - Postanowienia (Decisions, incl. *umorzenie* — discontinuance)
  - Uchwały (Resolutions)

## Endpoint Details

### SAOS (1985–2015)

- Search endpoint: `GET /api/search/judgments?courtType=CONSTITUTIONAL_TRIBUNAL`
- Detail endpoint: `GET /api/judgments/{id}`
- Full text in `textContent` field
- Pagination with max 100 items per page
- **Never send `sortingField`** — `JUDGMENT_DATE` ordering pushes the search endpoint
  from ~2–4s to 15–75s+ per page and it starts timing out (issue #1468).

### IPO (2016–present)

- Listing: `GET /ipo/SzukajDrukuj?cid=1&page=N` — the print view of the default
  search, i.e. every ruling sorted by date descending, 25 per page (~135 pages back
  to 1997). Requires the session cookies set by `GET /ipo/` then `GET /ipo/Szukaj?cid=1`;
  without them `/Szukaj` redirects to itself indefinitely.
- Document: `GET /ipo/Sprawa?cid=1&dokument={id}` — full text inside `<div id="tekst_{id}">`.
  The `sprawa` parameter is optional.
- **Transport: this host answers over HTTP/2 only.** An HTTP/1.1 request completes the
  TCP/TLS handshake and is then never answered — it hangs until the client times out —
  so `requests`/`urllib3` cannot reach it at all and the failure looks exactly like an
  IP block. Measured side by side: `curl --http2` returns 200 in 0.19s while
  `curl --http1.1` is still hanging at 20s. The scraper therefore shells out to
  `curl --http2`, which negotiates h2 via ALPN.

## Data Fields

- `case_number`: Official case reference (e.g., "K 7/94", "P 1/21")
- `date`: Date of the ruling (ISO 8601)
- `judgment_type`: SAOS — DECISION, SENTENCE, RESOLUTION, REASONS;
  IPO — the Polish label ("Wyrok", "Postanowienie - umorzenie", …)
- `judges`: List of judges with roles (presiding, reporting)
- `text`: Full text of the ruling
- `keywords`: Subject matter keywords (SAOS only)
- `referenced_regulations`: Laws and regulations cited (SAOS only)
- `subject`, `publication`: *Dotyczy* and *Miejsce publikacji* (IPO only)
- `fetch_path`: `"ipo"` or `"saos"` — which path produced the record

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — SAOS database, cite source.

## Usage

```bash
# Test SAOS API connectivity
python bootstrap.py test-api

# Test the IPO portal path (session, listing, full-text extraction)
python bootstrap.py test-ipo

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```
