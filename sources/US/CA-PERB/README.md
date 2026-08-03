# US/CA-PERB — California Public Employment Relations Board (Decisions)

Full text of every published **Board Decision** of the California Public
Employment Relations Board (PERB), California's quasi-judicial agency for
public-sector labor relations. PERB adjudicates unfair-practice charges,
representation disputes and related matters under the state's public-sector
labor statutes — **EERA** (public schools & community colleges), **HEERA**
(higher education), the **Dills Act** (state employees), the **MMBA** (local
government), the **Trial Court** and **Court Interpreter** Acts, **TEERA**,
and others. Each Board Decision resolves a specific contested case = `case_law`.

- **~4,005 decisions**, 1976–present (~3,900 born-digital PDFs in the media library).
- **Type:** `case_law`
- **Jurisdiction:** US-CA
- **Auth:** none · **CAPTCHA:** none

## Access

`perb.ca.gov` is a WordPress site. Every decision is a born-digital,
text-layer PDF under `/wp-content/uploads/decisionbank/`, enumerable via the
public WP REST API:

```
GET /wp-json/wp/v2/media?search=decisionbank&per_page=100&page={N}
```

Filenames are `decision-<NUM>.pdf` (e.g. `decision-2995e.pdf`,
`decision-2422H.pdf`), plus a few `order-<num>.pdf` / `<NUM>.pdf` "J"-series
items. `<NUM>` is the PERB Decision number followed by a one-letter sector
suffix (E = EERA, H = HEERA, M = MMBA, S = Dills Act, C = court employees).

Per-decision metadata (official issue date, a Description/Disposition summary,
canonical `/decision/{num}/` page URL) comes from the `decision` custom post
type (`/wp-json/wp/v2/decision`), joined to each PDF by slug. Text is always
extracted from the PDF via `common.pdf_extract` (no OCR needed).

## Usage

```bash
python bootstrap.py test-api            # connectivity + one-PDF extraction
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~3,900 decisions)
```

## Follow-up extensions

The same WP REST API also exposes `alj-decision` (proposed decisions of
Administrative Law Judges) and `fact-finder-report` custom post types with the
same `/decisionbank/` media mechanics — easy additional sources.

## License

[Public Domain (US Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — Decisions of the California Public Employment Relations Board are official California state-government works in the public domain under the government-edicts doctrine. No attribution required; commercial use permitted.
