# US/IL-IWCC — Illinois Workers' Compensation Commission, Commission-Level Decisions

Commission-level decisions of the Illinois Workers' Compensation Commission — the
panel decisions that review an arbitrator's award under §19(b)/§19(e) of the
Illinois Workers' Compensation Act. When the Commission affirms and adopts the
arbitrator, that award is attached to and forms part of the decision.

- **Type:** `case_law`
- **Jurisdiction:** US-IL
- **Index:** <https://iwcc.illinois.gov/resources/resources-for/decisions.html>
- **Auth:** none — plain GET, no JS, no CAPTCHA

## How it works

Decisions are not published one file per case. Each month the Commission posts a
single roll-up PDF holding every decision it issued that month (~40–60 decisions,
~400–1,000 pages, 8–30 MB). CompFile stamps a **"DECISION SIGNATURE PAGE"** cover
sheet in front of every decision, and that cover sheet is both the record
separator and the metadata source — it carries the WC case number, case name,
proceeding type, decision type, the official `NNIWCCNNNN` decision number, the
issuing Commissioner, both attorneys and the filing date.

`bootstrap.py` therefore:

1. reads the decisions page and collects every roll-up PDF,
2. downloads each one and extracts per-page text with PyMuPDF,
3. cuts the bundle at each signature page whose *Decision Type* is a Commission
   decision, and
4. emits one record per decision, its text running to the next such page.

The arbitrator's award attached behind a decision has its own signature page
marked **"Arbitration Decision"**. Those do *not* open a new record — they belong
to the decision they follow.

## Coverage and the OCR gap

Roll-ups live in two DAM folders, so both are matched: the dated
`/iwcc/documents/monthly-decisions/{YEAR}/{month}/` tree used from 2023 onward,
and the flat `/iwcc/resources/documents/` folder where the 2021–2022 CompFile
bundles still sit (identified by the `CompFile` marker in the filename).

| Period | Status |
|---|---|
| Apr 2021 – present | **Ingested.** Born-digital, signature pages present. |
| 2014 – early 2021 (`/monthly-decisions/archived/`) | **Skipped — needs OCR.** |

The archived bundles are page scans with no text layer at all (verified: 0 of 478
pages carry text in April 2019, 0 of 856 in August 2020), and the 2014 files have
only a poor OCR layer with no signature pages to split on, in ~210 MB-per-month
files. Recovering them needs an OCR pass, not a scraper change.

A few links on the page point at files that were never uploaded (e.g. the
May 2016 and January 2017 entries 404). Those are logged and skipped.

## Requirements

PyMuPDF (`fitz`) for text extraction. No credentials.

## Usage

```bash
python bootstrap.py test-api          # list roll-ups, split the newest one
python bootstrap.py bootstrap --sample # 15 sample records -> sample/
python bootstrap.py bootstrap          # full corpus
python bootstrap.py bootstrap-fast     # same corpus, concurrent (VPS wrapper)
```

## Record shape

`_id` is `US-IL-IWCC-{decision number}` (e.g. `US-IL-IWCC-26IWCC0128`), falling
back to case number + filing date for the corrected decisions posted without a
cover sheet. `text` holds the full decision body — typically 30,000–90,000
characters, including the attached arbitrator's award. `date` is the CompFile
filing date in ISO 8601.

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
decisions of an Illinois state adjudicative body are government edicts and are not
subject to copyright. No attribution required; commercial use permitted.
