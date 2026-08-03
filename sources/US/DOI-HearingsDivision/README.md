# US/DOI-HearingsDivision — Departmental Cases Hearings Division (Decisions)

Full-text decisions of the **Departmental Cases Hearings Division** (formerly
the "Hearings Division") of the U.S. Department of the Interior's **Office of
Hearings and Appeals (OHA)** — the trial-level tribunal whose administrative
law judges hold hearings and decide contested cases arising under Interior
programs: Bureau of Land Management grazing decisions and trespass, Surface
Mining (OSM) civil-penalty and permit cases, and other public-land enforcement
matters.

Each decision resolves a contested case (e.g. `ID-JFO-2020-006`), so the corpus
is **case_law**.

## Source

- Publisher: U.S. Department of the Interior, Office of Hearings and Appeals
- Database: Hearings Division Decisions 2000–Present (ISYS / Perceptive Enterprise Search)
- Endpoint: `https://www.oha.doi.gov:8080/`

## How it works

Same full-text ISYS portal and stateless HTTP **GET** flow as US/DOI-IBLA,
with `IW_DATABASE=Hearings Division Decisions 2000-Present` (POST to `/search/`
403s; GET works):

1. `GET /search/?IW_FIELD_NATURAL_LANGUAGE=<query>&IW_DATABASE=Hearings Division Decisions 2000-Present`
   → results page carrying a server-cached query **GUID** and the total document count.
2. `GET /isysquery/<GUID>/<start>-<end>/list/` → a page of hits (50 per page).
   Each hit N links its PDF at `/isysquery/<GUID>/<N>/doc/<filename>.pdf`.
3. `GET /isysquery/<GUID>/<N>/doc/<filename>.pdf` → the born-digital decision PDF.

Unlike the IBLA/IBIA databases (whose PDF filename is a clean reporter
citation), here the filename is a **descriptive name with spaces** (URL-encoded
as `%20`) that embeds the party, the Interior case reference and a
`dtd MM-DD-YYYY` decision date — e.g.
`Western Watersheds Project ID-JFO-2020-006 et al dtd 10-20-2021.pdf`. The
filename is URL-decoded and used for the dedup key, docket, date and title.

The corpus is enumerated by running broad seed queries (the single seed
"Bureau of Land Management" already returns ~1,162 documents), paging each
query's hits, and de-duplicating by filename. Full text is extracted from the
PDF with the shared `common.pdf_extract` helper.

Sibling of **US/DOI-IBLA** (Land Appeals merits), **US/DOI-IBLA-Orders**, and
**US/DOI-IBIA** (Indian Appeals) — same recipe, different `IW_DATABASE`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one full-text extract
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (U.S. Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Interior Departmental Cases Hearings Division are works of the U.S. federal government and are in the public domain (government-edicts doctrine). Commercial use permitted; no attribution required.
