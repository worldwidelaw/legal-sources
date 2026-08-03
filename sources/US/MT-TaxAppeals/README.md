# US/MT-TaxAppeals — Montana Tax Appeal Board (MTAB)

Final decisions and orders of the **Montana Tax Appeal Board**, the independent
quasi-judicial state agency that hears and decides all Montana state tax
appeals: individual income, corporation, natural-resource, centrally assessed,
motor-fuels and cigarette taxes, and every class of property tax (residential,
residential-recreational, commercial, agricultural, timber-land, personal
property, tax-exempt property, etc.). Each published document adjudicates a
specific taxpayer appeal, so the corpus is **case_law**.

## Source

- **Publisher:** Montana Tax Appeal Board (`mtab.mt.gov`)
- **Index:** https://mtab.mt.gov/decisions/
- **Type:** `case_law`
- **Auth:** none — no JavaScript, no CAPTCHA, no login

## How it works

1. `discover_documents()` fetches the decisions index and extracts its ~24
   subject-matter category subpages (relative slugs such as `incometax`,
   `residential-property`, `corptax`, `New-Decisions`).
2. Each category page is a server-rendered `<ul>`; every `<li>` ships an
   `<a href="{FILE}.pdf">Case Name v. MDOR</a>` followed by the docket
   number(s) (e.g. `IT-2024-18`, `PT-2025-4`). Recent decisions link a plain
   filename (resolved against `/decisions/`); older decisions link a
   `../_docs/decisions/{FILE}.pdf` path. Documents are de-duplicated by PDF URL
   across categories (a decision may appear under both its tax type and
   *New Decisions*).
3. The decision **date** is parsed from the PDF filename (`M.D.YY`,
   `M.D.YYYY`, `MM-DD-YYYY`, or a year-only token for the oldest decisions).
4. `_build_raw()` downloads each PDF and extracts full text with
   `common.pdf_extract` (born-digital text via pdfplumber). A 200-character
   guard skips the rare scanned image-only PDF (OCR fallback via `tesseract`
   if installed).

## Record schema

| field | description |
|-------|-------------|
| `_id` | `US/MT-TaxAppeals/{slug}` (slug derived from the PDF filename) |
| `_type` | `case_law` |
| `docket_number` | docket number(s) from the listing, e.g. `IT-2024-18` |
| `case_name` | party caption, e.g. *Neil Joseph Streber v. Department of Revenue* |
| `court` | `Montana Tax Appeal Board` |
| `title` | case name plus docket number |
| `text` | full decision text |
| `date` | decision date (ISO 8601) parsed from the filename |
| `url` | direct link to the decision PDF |
| `jurisdiction` | `US-MT` |

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Montana Tax Appeal Board are official Montana state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
