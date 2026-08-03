# US/MS-TaxNotices — Mississippi Department of Revenue: Notices & Technical Bulletins

Full text of the Mississippi Department of Revenue's published **Notices and
Technical Bulletins** — the Department's official interpretive tax guidance
(notices, technical bulletins, information bulletins and directives that explain
how Mississippi tax statutes and regulations apply). These are interpretive
**doctrine** issued by a state tax authority.

- **Source type:** `doctrine`
- **Jurisdiction:** US-MS (Mississippi)
- **Corpus size:** ~266 documents (as of 2026-07), 1984-style numbering onward
- **Full text:** yes — extracted from born-digital PDFs (no OCR needed)

## How it works

Site: `dor.ms.gov` (Drupal). The listing page

    https://www.dor.ms.gov/forms-resources/notices-technical-bulletins

is a server-side-rendered Drupal Views table. Each row carries structured
metadata columns — **Title, Date, Type, Tax Category, Division** — and links
directly to a born-digital PDF hosted on the same server under
`/sites/default/files/...`. Pagination is the standard Views query parameter
`?items_per_page=25&page=N` (page 0 is the first page). The scraper walks pages
until two consecutive empty pages, downloads each PDF, and extracts full text
with `common.pdf_extract` (pdfplumber). No JavaScript, no CAPTCHA, no auth.

The document number (e.g. `72-26-13`) is parsed from the file name when present.

> The host presents a slightly misordered TLS certificate chain, so bytes are
> fetched with curl's permissive TLS (equivalent to `verify=False`). This is a
> server misconfiguration, not a bypass of any access control — every document
> is public.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # Fetch ~12 sample docs
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Notices and technical bulletins of the Mississippi Department of Revenue are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
