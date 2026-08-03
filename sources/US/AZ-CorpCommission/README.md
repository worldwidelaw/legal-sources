# US/AZ-CorpCommission — Arizona Corporation Commission Decisions & Orders

Full text of **Decisions** and **Procedural Orders** issued by the Arizona
Corporation Commission (ACC), the state's public-utility regulator. Each
Decision/Order adjudicates a specific utility docket (electric, natural-gas,
water/sewer, telecommunications, pipeline/railroad safety) — rate cases,
certificates of convenience & necessity (CC&N), complaints, financings and
other matters. These are administrative adjudications of specific cases =
**case_law**.

## Source

- **eDocket web app:** https://edocket.azcc.gov/
- **Public JSON API:** `https://efiling.azcc.gov/api/edocket/`
- **Document images:** `https://docket.images.azcc.gov/{imageNumber}.pdf`

## How it works

The eDocket Angular front-end is backed by a **public, no-auth JSON API**.

1. `POST /api/edocket/documentSearchRequest` with a filing-date range and a
   document code returns document metadata (documentID, imageNumber,
   filedDate, and `docketSummaries` with the docket number, company/party
   name, description and case type). The full field set must be present in
   the request body; results page through `totalRowCount` via
   `currentPageIndex` / `rowsToSkip`.
2. The adjudicative documents are two document codes:
   - **723 = Decision** (substantive Commission Decisions)
   - **727 = Procedural Order** (orders setting hearings, granting
     extensions, closing dockets, etc.)
3. Each document's born-digital PDF is at
   `https://docket.images.azcc.gov/{imageNumber}.pdf`. Full text is
   extracted with PyMuPDF (`fitz`); older image-only scans fall back to
   Tesseract OCR. The Decision number is parsed from the body; the docket
   number comes from the authoritative eDocket metadata.

`fetch_all()` walks filing years newest-first (~1990 floor), so the sample
pull draws from clean modern born-digital Decisions.

## Corpus

~380–400 Decisions per year plus Procedural Orders, spanning the early 1990s
to present (tens of thousands of documents).

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — Arizona)](https://www.law.cornell.edu/uscode/text/17/105) — ACC Decisions and Orders are official state government edicts in the public domain; no attribution required, commercial use permitted.
