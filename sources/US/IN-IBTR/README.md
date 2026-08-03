# US/IN-IBTR — Indiana Board of Tax Review (Final Determinations)

Full text of the **final determinations** of the **Indiana Board of Tax
Review (IBTR)** — the Indiana state administrative tribunal that adjudicates
appeals of property-tax assessments. Each determination ("Findings &
Conclusions") resolves a specific dispute between a taxpayer and a county
assessor over the assessment, exemption, deduction or credit of real or
personal property (reviewing a county Property Tax Assessment Board of
Appeals). Each determination resolves a specific controversy, so the corpus
is **case_law**.

## Source

- **Index:** https://www.in.gov/ibtr/decisions/
- **Documents:** born-digital text-layer PDFs under `https://www.in.gov/ibtr/files/*.pdf`
- **Coverage:** 2002–present (~270 monthly decision pages)
- **Access:** plain HTML index + PDFs; no JavaScript, no CAPTCHA, no auth. `in.gov` is reachable from ordinary clients.

## Strategy

1. `GET /ibtr/decisions/` and collect every `/ibtr/decisions/<slug>` month/decision
   sub-page whose slug carries a 4-digit year (slug formats vary by era).
2. `GET` each month page (an HTML table: Petition # | Issued | Petitioner link |
   Issues) and collect every `/ibtr/files/*.pdf` link, skipping
   petition-listing / cause-list / calendar artefacts. Each PDF is associated
   with the authoritative "Issued" `M/D/YYYY` date in the preceding cell.
3. Download each PDF and extract its text layer via `common.pdf_extract`.

## Usage

```bash
python3 bootstrap.py test-api            # connectivity / extraction test
python3 bootstrap.py bootstrap --sample  # ~12 sample determinations
python3 bootstrap.py bootstrap           # full pull (all determinations)
python3 bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Output schema (case_law)

`_id`, `_source`, `_type`, `_fetched_at`, `slug`, `docket_number` (petition
number), `court`, `petitioner`, `title`, `text` (full determination text),
`url`, `date` (issued date, ISO 8601), `jurisdiction` (`US-IN`).

## License

[Public Domain — US Government Work](https://www.law.cornell.edu/uscode/text/17/105) — final determinations of the Indiana Board of Tax Review are official quasi-judicial government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
