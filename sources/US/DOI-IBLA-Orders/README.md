# US/DOI-IBLA-Orders — Interior Board of Land Appeals (Dispositive Orders)

Full-text **dispositive orders** of the **Interior Board of Land Appeals
(IBLA)**, the appellate board within the U.S. Department of the Interior's
**Office of Hearings and Appeals (OHA)** that reviews Bureau of Land Management
(BLM) and other Interior bureau decisions on public-land use and resources.

Unlike the reported merits decisions (see **US/DOI-IBLA**), these are the orders
that dispose of a docketed appeal without a full reported opinion — dismissals
for mootness, untimeliness, lack of standing or jurisdiction; orders granting
withdrawal or approving settlement; stay rulings; and the like.

Each order resolves a docketed appeal (e.g. `IBLA 2021-123`), so the corpus is
**case_law**.

## Source

- Publisher: U.S. Department of the Interior, Office of Hearings and Appeals
- Database: IBLA Dispositive Orders (ISYS / Perceptive Enterprise Search)
- Endpoint: `https://www.oha.doi.gov:8080/`

## How it works

Same full-text ISYS portal and stateless HTTP **GET** flow as US/DOI-IBLA,
with `IW_DATABASE=IBLA Dispositive Orders` (POST to `/search/` 403s; GET works):

1. `GET /search/?IW_FIELD_NATURAL_LANGUAGE=<query>&IW_DATABASE=IBLA Dispositive Orders`
   → results page carrying a server-cached query **GUID** and the total document count.
2. `GET /isysquery/<GUID>/<start>-<end>/list/` → a page of hits (50 per page).
   Each hit N links its PDF at `/isysquery/<GUID>/<N>/doc/<filename>.pdf`, where
   the filename is the appeal docket (`2021-0123.pdf` = IBLA 2021-123).
3. `GET /isysquery/<GUID>/<N>/doc/<filename>.pdf` → the born-digital order PDF.

The corpus is enumerated by running broad seed queries (the single seed
"Bureau of Land Management" already returns ~3,678 documents), paging each
query's hits, and de-duplicating by the PDF filename. Full text is extracted
from the PDF with the shared `common.pdf_extract` helper.

Sibling of **US/DOI-IBLA** (merits decisions) and **US/DOI-IBIA** (Indian
Appeals) — same recipe, different `IW_DATABASE`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one full-text extract
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (U.S. Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — orders of the Interior Board of Land Appeals are works of the U.S. federal government and are in the public domain (government-edicts doctrine). Commercial use permitted; no attribution required.
