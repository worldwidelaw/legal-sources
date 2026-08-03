# UK/ScotCourts — Scottish Courts and Tribunals Service, Judgments

Full-text judgments and opinions from Scotland's superior/appellate courts and
the Upper Tribunal for Scotland, published by the **Scottish Courts and Tribunals
Service (SCTS)** at <https://www.scotcourts.gov.uk/judgments/>.

This is the authoritative Scottish superior-court case-law corpus for the
**GB-SCT** jurisdiction. It is **not** covered by `UK/CaseLaw` — the National
Archives "Find Case Law" service indexes England & Wales superior courts and
reserved UK tribunals only; Scottish court judgments are not on that service. It
is also distinct from the devolved Scottish first-tier tribunal chambers already
in the repo (`UK/ScotHousingChamber`, `UK/ScotTaxChamber`,
`UK/ScotLocalTaxChamber`, `UK/ScotHealthEducationChamber`).

## Courts covered (single "Judgments" index, ~13,120 judgments)

- **Court of Session** (Inner House / Outer House) — supreme civil court — `CSIH` / `CSOH`
- **High Court of Justiciary** — supreme criminal court — `HCJAC` / `HCJ`
- **Sheriff Appeal Court** (Civil & Criminal)
- **Sheriff Courts** (Civil & Criminal), across all sheriffdoms — `SC <place> N`
- **National Personal Injury Court**
- **Upper Tribunal for Scotland** (all chambers: Social Security, Housing &
  Property, Local Taxation, General Regulatory) — `UT N`

## How it works

The `/judgments/` page is a client-side (Vue) search app that reads its API base
from a `data-base-url` attribute:

```
https://api.pa.web.scotcourts.gov.uk/web
```

Two operations (an autorest/`@azure` client baked into the page's JS bundle):

- `GET  /web/definition/{contentId}` → index configuration (`indexType`, `limit`)
- `POST /web/search` → paginated results

Search request body:

```json
{"query": "", "filters": [], "page": N,
 "indexType": "Judgments", "category": "", "limit": 50}
```

Each result carries structured metadata plus a relative `documentLink` to a
born-digital decision PDF on the main site:

```json
{"title": "...", "documentLink": "/media/<id>/<citation>-....pdf",
 "date": "2026-07-17T00:00:00Z", "court": ["Court of Session"],
 "sheriffdom": [...], "judges": [...], "additionalDate": "...", "tags": [...]}
```

The scraper pages `1..pagination.page.total` (limit 50, newest first), downloads
each PDF, and extracts full text with **PyMuPDF** (pdfplumber/pypdf fallback) —
the PDFs are born-digital, so no OCR is needed. Metadata (title, court, judges,
date, neutral citation) comes from the API + PDF filename; full text from the
PDF. One record per judgment PDF.

## Usage

```bash
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent judgments)
python bootstrap.py test               # Quick connectivity test
```

## Data

- ~13,120 full-text judgments (growing daily).
- Language: English.
- Auth: none (free public access).

## License

> ⚠️ **Commercial use restricted.** See terms below.

[SCTS website terms of use](https://www.scotcourts.gov.uk/terms-of-use) —
SCTS permits reproduction of judgments/decisions for personal and in-house use
but restricts commercial re-use without consent (same basis as
`UK/ScotHousingChamber`, `UK/ScotTaxChamber`, `UK/ScotLocalTaxChamber`). Not OGL.
Attribution to the Scottish Courts and Tribunals Service is expected.
