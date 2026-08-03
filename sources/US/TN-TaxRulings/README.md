# US/TN-TaxRulings — Tennessee Department of Revenue Letter Rulings

Full text of the redacted **Letter Rulings** published by the Tennessee
Department of Revenue. A Letter Ruling is the Department's written
interpretation and application of Tennessee tax law to a specific set of
facts furnished by a taxpayer (Tenn. Code Ann. § 67-1-109). The redacted
rulings are published openly by the Department for informational use, and
are official state-government interpretive guidance — `doctrine`, not
adjudications of a contested case.

## Source

- **Index:** https://www.tn.gov/revenue/tax-resources/legal-resources/tax-rulings.html
- **Per tax-type pages:** `…/tax-rulings/{type}.html` (sales-and-use-tax,
  franchise---excise-tax, business-tax, etc.)
- **Documents:** `https://www.tn.gov/content/dam/tn/revenue/documents/rulings/{category}/{number}.pdf`

## How it works

1. Each tax-type page is a server-rendered rich-text widget whose body
   holds the ruling links **HTML-entity-encoded**; they only appear after
   `html.unescape()`. The scraper unescapes each page and regexes out every
   `…/documents/rulings/…/*.pdf` anchor (URL + link text), deduped by URL
   (~598 rulings across the tax-type pages).
2. Each ruling PDF is downloaded and its full text extracted via the shared
   OOM-hardened `common.pdf_extract` helper (pdfplumber → pypdf → OCR
   fallback). A `<200`-char guard skips the rare image-only/empty scan.
3. The issue date is parsed from the ruling body (month-name date near the
   top) when present, else derived from the `YY-` prefix of the ruling
   number (`YY<=30` → `20YY`, else `19YY`).

No JavaScript, no CAPTCHA, no auth.

> **Note:** `curl` hits an HTTP/2 quirk on `www.tn.gov` (returns HTTP 000);
> python-`requests` / the project `HttpClient` fetch the same URLs fine.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all tax types)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Tennessee state tax guidance)](https://www.law.cornell.edu/uscode/text/17/105) — Redacted
Letter Rulings of the Tennessee Department of Revenue are official
state-government works in the public domain under the government-edicts
doctrine. No attribution required; commercial use permitted.
