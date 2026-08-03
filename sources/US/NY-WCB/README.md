# US/NY-WCB — New York Workers' Compensation Board — Board Decisions

Full text of the openly-published decisions of the **New York State Workers'
Compensation Board (WCB)** — the quasi-judicial agency that adjudicates
workers'-compensation, disability-benefits and paid-family-leave claims under
the New York Workers' Compensation Law.

On administrative appeal from a Workers' Compensation Law Judge (WCLJ), a
three-member **Board Panel** — and, on Mandatory or discretionary **Full Board
Review**, the entire Board — issues a written *Memorandum of Decision*
resolving the contested case. Each such decision resolves a specific case =
`case_law`.

## Data source

Two openly-browsable decision indexes on `wcb.ny.gov` (no auth, no CAPTCHA):

| Set | Index | Document form |
|-----|-------|---------------|
| Select Board Panel Decisions | `/content/main/Decisions/board-panel-decisions.jsp` | one server-rendered HTML page per decision (`.../board-panel-decisions/{Matter...}.jsp`); body inside `<div id="mainContent">` |
| Significant Full Board Decisions | `/content/main/Decisions/board-decisions.jsp` | born-digital PDFs under `/content/main/Decisions/{YYYYMon}/*.pdf` |

The scraper reads both indexes, downloads each HTML decision page or PDF,
extracts the full decision text (`mainContent` for HTML, `common.pdf_extract`
for PDF), and parses the matter caption, WCB case number, NY Wrk Comp neutral
citation and decision date.

### Scope note

The Board's **complete** decision corpus is served only through the
case-number-keyed **eCase** system (per-claim lookup, not openly enumerable —
there is no bulk or list endpoint). Only these curated *Board Panel* and
*Significant Full Board* decision sets are published as browsable full text, so
the manifest marks `US-NY` jurisdiction scope as `partial`.

## Usage

```bash
python bootstrap.py test-api           # connectivity / extraction check
python bootstrap.py bootstrap --sample # save ~12 samples to sample/
python bootstrap.py bootstrap          # full pull (~29 decisions)
python bootstrap.py bootstrap-fast     # alias for full pull (VPS wrapper)
```

Requires an interpreter with `PyMuPDF` (fitz) for the Full Board PDFs
(the system `/usr/bin/python3` has it here).

## Record schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `record_id`, `issuer`,
`title`, `citation`, `case_number`, `text` (full decision), `url`, `date`
(ISO 8601), `jurisdiction` (`US-NY`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the New York State Workers' Compensation Board are official works of New York state government (edicts of a quasi-judicial government body) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
