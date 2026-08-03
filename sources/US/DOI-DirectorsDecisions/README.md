# US/DOI-DirectorsDecisions — Director's Decisions (U.S. DOI, Office of Hearings and Appeals)

Full-text **Director's Decisions** of the U.S. Department of the Interior's
**Office of Hearings and Appeals (OHA)** — decisions issued by (or on the
authority of) the Director of OHA, primarily contested-case decisions in Indian
probate and other Interior matters exercised under the Secretary's delegated
authority, published in the **"OHA" reporter** (e.g. `40 OHA 202`).

Each decision resolves a specific contested case, so the corpus is **case_law**.

## Source

- Publisher: U.S. Department of the Interior, Office of Hearings and Appeals
- Database: Directors Decisions 1996–Present (ISYS / Perceptive Enterprise Search)
- Endpoint: `https://www.oha.doi.gov:8080/`

## How it works

Same full-text ISYS portal and stateless HTTP **GET** flow as US/DOI-IBLA, with
`IW_DATABASE=Directors Decisions 1996-Present` (POST to `/search/` 403s; GET
works):

1. `GET /search/?IW_FIELD_NATURAL_LANGUAGE=<query>&IW_DATABASE=Directors Decisions 1996-Present`
   → results page carrying a server-cached query **GUID** and the total document count.
2. `GET /isysquery/<GUID>/<start>-<end>/list/` → a page of hits (50 per page).
   Each hit N links its PDF at `/isysquery/<GUID>/<N>/doc/<filename>.pdf`.
3. `GET /isysquery/<GUID>/<N>/doc/<filename>.pdf` → the born-digital decision PDF.

The PDF filename embeds the **OHA reporter citation**, the party caption and a
trailing **`M-D-YY`** decision date — e.g.
`40OHA202 IN RE GRASSHOPPER SUPPRESSION 5-14-10.pdf`. The filename is
URL-decoded and used for the dedup key, citation, date and title. A handful of
the database's entries are bare CFR reference documents (filenames like
`25cfr2.pdf`) rather than decisions; these are skipped.

The corpus is enumerated by running broad seed queries (the single seed
"Bureau of Land Management" already returns ~503 documents), paging each query's
hits, and de-duplicating by filename. Full text is extracted from the PDF with
the shared `common.pdf_extract` helper.

Sibling of **US/DOI-IBLA** (Land Appeals merits), **US/DOI-IBLA-Orders**,
**US/DOI-IBIA** (Indian Appeals) and **US/DOI-HearingsDivision** — same recipe,
different `IW_DATABASE`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one full-text extract
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (U.S. Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — Director's Decisions of the Interior Office of Hearings and Appeals are works of the U.S. federal government and are in the public domain (government-edicts doctrine). Commercial use permitted; no attribution required.
