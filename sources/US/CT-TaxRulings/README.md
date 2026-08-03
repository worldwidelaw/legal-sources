# US/CT-TaxRulings — Connecticut Department of Revenue Services Rulings

Full text of the **Rulings** issued by the Connecticut Department of
Revenue Services (DRS) — the Department's written interpretation of how
Connecticut tax law applies to a described set of facts (sales & use tax,
corporation business tax, admissions tax, etc.). Published openly on the
ct.gov portal. These are official state-government interpretive guidance —
`doctrine`, not adjudications of a contested case.

## Source

- **Library:** https://portal.ct.gov/DRS/Publications/Rulings
- **Year index:** `…/Rulings/{YEAR}/{YEAR}-Rulings`
- **Ruling page:** `…/Rulings/{YEAR}/ruling-{number}-{slug}` (full text in HTML)

## How it works

1. Walk each year index (1990–present), collecting every ruling-page URL
   (deduped by URL). ~255 rulings are online (1990–2019).
2. Fetch each ruling page and extract the `<div class="content">` body
   (cut at `</main>`), strip tags, decode entities. A retired/"Oops" page
   (HTTP 200 with an error body) or a `<200`-char page is skipped.
3. The ruling number and title come from the page `<h1>`; the issue date
   is parsed from the ruling body (month-name date) when present, else
   derived from the ruling number's year.

No JavaScript, no CAPTCHA, no auth. Distinct from **US/CT-TaxAppeals**
(the Connecticut Superior Court Tax & Administrative Appeals Session —
`case_law`).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all years)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Connecticut state tax guidance)](https://www.law.cornell.edu/uscode/text/17/105) — Rulings
of the Connecticut Department of Revenue Services are official
state-government works in the public domain under the government-edicts
doctrine. No attribution required; commercial use permitted.
