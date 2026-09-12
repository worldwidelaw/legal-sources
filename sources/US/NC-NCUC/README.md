# US/NC-NCUC — North Carolina Utilities Commission Orders

Full text of **Orders and decisions** issued by the **North Carolina Utilities
Commission (NCUC)** adjudicating utility dockets — electric, natural gas,
water/sewer and telephone matters: general rate cases, fuel and rider
adjustments, certificates of public convenience and necessity, complaints,
integrated resource plans, securitization proceedings, competitive-procurement
(RFP) dockets and rulemakings.

Each Commission Order is an administrative adjudication / edict of a specific
docket → **`case_law`**.

## Access

The NCUC document portal at **`starw1.ncuc.gov`** (the "STAR" filing system) is
fronted by **Cloudflare**, which returns an *"Attention Required!"* HTTP 403
managed-challenge page to every datacenter / non-browser vantage (verified 403
from this build vantage and from Anthropic's fetch vantage, 2026-07-27). Both
the searchable Orders / DocketDetails listing pages **and** the `ViewFile.aspx`
PDF handler are behind the challenge, so the live host cannot be enumerated or
downloaded without browser automation / a residential Cloudflare solver.

The corpus is therefore read from the **Internet Archive (Wayback Machine)**,
which has a large crawl (~14,000 distinct order/filing PDFs) of the
Commission's public documents served at
`starw1.ncuc.gov/NCUC/ViewFile.aspx?Id={GUID}`.

- **`fetch_all()`** enumerates every archived `ViewFile.aspx?...&Id={GUID}` PDF
  snapshot via the Wayback **CDX API** (prefix match, mimetype
  `application/pdf`, statuscode 200) and de-duplicates on the document GUID.
- **`normalize()`** downloads the raw archived PDF
  (`https://web.archive.org/web/{ts}id_/{original}`) and extracts full text via
  **fitz / PyMuPDF** (Tesseract OCR fallback for the rare scanned document). The
  ViewFile URL carries no metadata, so each PDF is classified from its own
  text: a document is kept only if it has the NCUC caption header **and** either
  an ordering/decretal clause (*"IT IS, THEREFORE, ORDERED"*, *"BY ORDER OF THE
  COMMISSION"*, …) or an `ORDER` caption doc-type on page 1. Party filings
  (testimony, comments, motions, complaints, briefs, applications) are dropped.
- The docket number, order caption (title) and issued date are parsed from the
  PDF text.

## Usage

```bash
python bootstrap.py test-api            # connectivity / extraction test
python bootstrap.py bootstrap --sample  # ~12 sample orders
python bootstrap.py bootstrap           # full pull (all orders)
python bootstrap.py bootstrap-fast      # high-throughput full pull (VPS)
```

## Fields

| Field | Description |
|-------|-------------|
| `_id` | `US/NC-NCUC/{document GUID}` |
| `docket_number` | e.g. `E-7, SUB 1276`, `W-354, SUB 384`, `SP-13695, SUB 1` |
| `title` | docket number + order caption |
| `text` | full text of the Order |
| `date` | order issued date (ISO 8601) |
| `url` | canonical `ViewFile.aspx` URL (Cloudflare-blocked reference) |
| `archive_url` | Wayback raw-PDF URL the text came from |

## License

[Public Domain — U.S. Government Work (17 U.S.C. § 105 analogue)](https://www.law.cornell.edu/uscode/text/17/105) — North Carolina Utilities Commission Orders are official state government edicts in the public domain. No attribution required; commercial use permitted.
