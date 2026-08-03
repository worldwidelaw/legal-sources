# US/DOI-IBIA — Interior Board of Indian Appeals (Decisions)

Full-text decisions of the **Interior Board of Indian Appeals (IBIA)**, the
appellate board within the U.S. Department of the Interior's **Office of
Hearings and Appeals (OHA)** that decides appeals in Indian-affairs matters —
appeals from Bureau of Indian Affairs (BIA) decisions, Indian probate (estates
of deceased Indians), tribal enrollment and trust-land disputes,
tribal-government and election challenges, Indian Self-Determination Act
contract disputes, and related questions.

Each decision resolves a docketed appeal (e.g. `IBIA 09-5-A`) and is reported
by citation (e.g. `48 IBIA 279`), so the corpus is **case_law**.

## Source

- Publisher: U.S. Department of the Interior, Office of Hearings and Appeals
- Database: IBIA Decisions 1970–Present (ISYS / Perceptive Enterprise Search)
- Endpoint: `https://www.oha.doi.gov:8080/`

## How it works

The OHA search is a full-text ISYS index reachable over plain HTTP **GET**
(POST to `/search/` returns 403; GET works):

1. `GET /search/?IW_FIELD_NATURAL_LANGUAGE=<query>&IW_DATABASE=IBIA Decisions 1970-Present`
   → results page carrying a server-cached query **GUID** and the total document count.
2. `GET /isysquery/<GUID>/<start>-<end>/list/` → a page of hits (50 per page).
   Each hit N links its source PDF at `/isysquery/<GUID>/<N>/doc/<citation>.pdf`,
   where the filename **is** the reporter citation (`48ibia279.pdf` = 48 IBIA 279).
3. `GET /isysquery/<GUID>/<N>/doc/<citation>.pdf` → the born-digital decision PDF.

The corpus is enumerated by running broad seed queries (the single seed
"Bureau of Indian Affairs" already returns ~3,962 documents — effectively the
whole database, since every IBIA decision reviews a BIA action), paging each
query's hits, and de-duplicating by citation. Full text is extracted from the
PDF with the shared `common.pdf_extract` helper. The decision date is read
from the parenthetical after the citation in the header (`48 IBIA 279
(02/13/2009)`).

Sibling to **US/DOI-IBLA** (Land Appeals) — same recipe, different
`IW_DATABASE`. Other Interior boards use the same recipe (Directors Decisions,
Hearings Division, A&M 1920–1970, Land/Interior Decisions, IBLA Dispositive
Orders).

## Usage

```bash
python bootstrap.py test-api            # connectivity + one full-text extract
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (U.S. Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Interior Board of Indian Appeals are works of the U.S. federal government and are in the public domain (government-edicts doctrine). Commercial use permitted; no attribution required.
