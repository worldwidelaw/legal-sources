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

## Vantage fallback (issue #1234)

`www.tn.gov` TLS-resets or read-times-out connections from every **non-US
vantage** tested — both the Hetzner fleet IPs and the build machine get
`ECONNRESET` on the handshake — so a live-only run off a US residential IP
yields nothing (and, before this fallback, spent ~22h looping on retries).

Every fetch is therefore **live-first with an Internet Archive fallback**:

```
https://web.archive.org/web/3000id_/{url}
```

(`3000` = latest capture, `id_` = raw bytes, no IA banner injection.) The
archived tax-type pages list **595 unique ruling anchors** — the same corpus
the live pages list — and **589 of those PDFs have a capture**.

After three consecutive live failures with no live success the scraper
latches into archive-only mode (~90s) and stops paying the origin's timeout
on every remaining URL. From a US/residential vantage the live path answers
first and the archive is never touched.

> **Gotcha:** many ruling PDFs are stored as Wayback *revisit* records, which
> CDX reports with `statuscode` `-`, not `200`. Filtering CDX on
> `statuscode:200` undercounts coverage by half (301 URLs vs 656) even though
> the archive replays those captures fine.

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
