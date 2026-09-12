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

Four openly-browsable decision indexes on `wcb.ny.gov` (no auth, no CAPTCHA):

| Set | `decision_type` | Index | Document form |
|-----|-----------------|-------|---------------|
| Select Board Panel Decisions | `board_panel` | `/content/main/Decisions/board-panel-decisions.jsp` | one server-rendered HTML page per decision (`.../board-panel-decisions/{Matter...}.jsp`); body inside `<div id="mainContent">` |
| Significant Full Board Decisions | `full_board` | `/content/main/Decisions/board-decisions.jsp` | born-digital PDFs under `/content/main/Decisions/{YYYYMon}/*.pdf` |
| COVID-19 Decisions | `board_panel_covid` | `/content/main/Decisions/covid-19-decisions.jsp` | HTML pages under `.../covid-19-decisions/{matter-...}.jsp` |
| Appellate Court Decisions | `appellate_court` | `/content/main/Decisions/appellate-court-decisions.jsp` | Appellate Division, Third Department memoranda and orders deciding appeals from the Board, as PDFs under `.../court-decisions/*.pdf` |

The scraper reads all four indexes, downloads each HTML decision page or PDF,
extracts the full decision text (`mainContent` for HTML, `common.pdf_extract`
for PDF), and parses the matter caption, WCB case number, NY Wrk Comp neutral
citation and decision date.

### Historical enumeration (issue #1393)

`board-decisions.jsp` shows only the few most recent months: each time the
Board publishes a new set of Significant Full Board decisions it **deletes**
the previous ones, so every earlier month directory now returns HTTP 404. Read
from the live indexes alone the corpus is frozen at ~29 documents.

The scraper therefore also sweeps the Internet Archive CDX index for
`wcb.ny.gov/content/main/Decisions*`, which recovers the deleted series back to
the 2020 `.jsp` generation. Every document is fetched **live first** and only
replayed from `https://web.archive.org/web/{timestamp}id_/{url}` when the live
URL is gone, so a re-linked decision is always taken from the Board itself.

Discovery now yields **~574** unique decisions (411 appellate court, 131 Full
Board, 25 Board Panel, 7 COVID-19), of which ~137 exist only in the archive.

### Scope note

The Board's **complete** decision corpus is served only through the
case-number-keyed **eCase** system (per-claim lookup, not openly enumerable —
there is no bulk or list endpoint). Only these curated decision sets are
published as browsable full text, so the manifest marks `US-NY` jurisdiction
scope as `partial`.

## Usage

```bash
python bootstrap.py test-api           # connectivity / extraction check
python bootstrap.py bootstrap --sample # save ~12 samples to sample/
python bootstrap.py bootstrap          # full pull (~574 decisions)
python bootstrap.py bootstrap-fast     # alias for full pull (VPS wrapper)
```

Requires an interpreter with `PyMuPDF` (fitz) for the Full Board PDFs
(the system `/usr/bin/python3` has it here).

## Record schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `record_id`, `issuer`,
`decision_type`, `title`, `citation`, `case_number`, `text` (full decision),
`url`, `date` (ISO 8601), `jurisdiction` (`US-NY`).

`_id` is `US/NY-WCB/{matter-slug}` for the Board Panel index (unchanged), and
`{YYYYMon}-`, `covid-` or `court-` prefixed for the other three series so that
a matter republished across months — or published as both `.jsp` and `.pdf` —
does not collide.

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the New York State Workers' Compensation Board are official works of New York state government (edicts of a quasi-judicial government body) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
