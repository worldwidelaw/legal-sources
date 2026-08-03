# US/VT-TaxDecisions — Vermont Department of Taxes: Formal Rulings

Full text of the **Formal Rulings** published by the **Vermont Department of
Taxes** — redacted interpretive rulings issued to a requesting taxpayer and
published for general guidance under 32 V.S.A. These are interpretive
**doctrine**.

> **Scope note (2026-07-02):** the Department's separate **Determinations**
> collection (adjudications = case_law) has been removed from the site (the
> `/tax-law-and-guidance/determinations` path and its taxonomy term page now
> 404 / render empty, and the master `/documents` library does not carry the
> Determination or Formal Ruling categories), so this source is doctrine-only.
> Each ruling node page renders its full text inline in the body field, so no
> PDF download is required.

## Data access

`tax.vermont.gov` is a Drupal site. The per-category browsing pages
(`/tax-law-and-guidance/determinations`, `/document-categories/determination`)
render their listing through a CSRF-gated AJAX Views display — empty in static
HTML, and the `/views/ajax` token route returns 403 headless. The reliable
headless path is the **master document library**:

```
https://tax.vermont.gov/documents?page=0..N
```

The `documents` view renders every document row **server-side** (~14 rows per
page across ~467 pages, covering all document categories). Each data `<tr>`
carries:

- `views-field-title` → node link + human title
- `views-field-field-file` → the actual file href (`/sites/tax/files/documents/*.pdf`)
- `views-field-field-doc-category` → the category link (`/document-categories/{slug}`)

The scraper sweeps every page, keeps only rows whose category is
`determination` (→ case_law) or `formal-ruling` (→ doctrine), then downloads
each PDF and extracts its born-digital text layer via `common.pdf_extract`.

No JavaScript, no CAPTCHA, no authentication.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Record schema

Each normalized record contains `_id`, `_source`, `_type`
(`case_law` for determinations, `doctrine` for formal rulings),
`_fetched_at`, `title`, `text` (full PDF text), `date`, `url`, `category`,
`issuer`, `node_url`, and `jurisdiction` (`US-VT`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — determinations and formal rulings of the Vermont Department of Taxes are official state-government works in the public domain under the government-edicts doctrine. No attribution required; commercial use permitted.
