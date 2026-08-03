# US/DOI-IBLA — Interior Board of Land Appeals (Decisions)

Full-text decisions of the **Interior Board of Land Appeals (IBLA)**, the
appellate board within the U.S. Department of the Interior's **Office of
Hearings and Appeals (OHA)** that decides appeals from Bureau of Land
Management (BLM) and other Interior bureau decisions on the use and
disposition of public lands and their resources — grazing, oil & gas and
mineral leasing, mining claims, rights-of-way, surface management, wild
horse & burro, land patents, and related matters.

Each decision resolves a docketed appeal (e.g. `IBLA 98-287`) and is
reported by citation (e.g. `157 IBLA 230`), so the corpus is **case_law**.

## Source

- Publisher: U.S. Department of the Interior, Office of Hearings and Appeals
- Database: IBLA Decisions 1970–Present (ISYS / Perceptive Enterprise Search)
- Endpoint: `https://www.oha.doi.gov:8080/`

## How it works

The OHA search is a full-text ISYS index reachable over plain HTTP **GET**
(POST to `/search/` returns 403; GET works):

1. `GET /search/?IW_FIELD_NATURAL_LANGUAGE=<query>&IW_DATABASE=IBLA Decisions 1970-Present`
   → results page carrying a server-cached query **GUID** and the total hit count.
2. `GET /isysquery/<GUID>/<start>-<end>/list/` → a page of hits (50 per page).
   Each hit N links its source PDF at `/isysquery/<GUID>/<N>/doc/<citation>.pdf`,
   where the filename **is** the reporter citation (`157IBLA230.pdf` = 157 IBLA 230).
3. `GET /isysquery/<GUID>/<N>/doc/<citation>.pdf` → the born-digital decision PDF.

The corpus is enumerated by running broad seed queries (the single seed
"Bureau of Land Management" already returns ~88,954 hits — effectively the
whole database, since every IBLA decision reviews a BLM action), paging each
query's hits, and de-duplicating by citation. Full text is extracted from the
PDF with the shared `common.pdf_extract` helper.

Sibling Interior boards use the same recipe with a different `IW_DATABASE`
(IBIA Indian Appeals, Directors Decisions, Hearings Division, A&M 1920–1970,
Land/Interior Decisions, IBLA Dispositive Orders).

## Usage

```bash
python bootstrap.py test-api            # connectivity + one full-text extract
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (U.S. Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Interior Board of Land Appeals are works of the U.S. federal government and are in the public domain (government-edicts doctrine). Commercial use permitted; no attribution required.
