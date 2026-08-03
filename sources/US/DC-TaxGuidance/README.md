# US/DC-TaxGuidance — DC Office of Tax and Revenue: Tax Notices, Rulings & Declaratory Orders

Full text of the District of Columbia Office of Tax and Revenue's (OTR)
published interpretive tax guidance — **OTR Tax Notices, Sales Tax Notices, Tax
Rulings, Private Letter Rulings, Declaratory Orders** and combined-reporting
notices. These are the DC tax authority's official interpretive guidance on how
DC tax law applies — interpretive **doctrine**.

- **Source type:** `doctrine`
- **Jurisdiction:** US-DC (District of Columbia)
- **Corpus size:** ~150 documents across all guidance categories
- **Full text:** yes — extracted from born-digital PDFs (no OCR needed)

## How it works

Site: `otr.cfo.dc.gov` (Drupal). Each guidance category has a landing page under
`/page/...`:

- `/page/otr-tax-notices-and-guidance` — OTR Tax Notices
- `/page/sales-tax-information-and-guidance` — Sales Tax Notices
- `/page/otr-tax-rulings` — Tax Rulings
- `/page/otr-private-letter-rulings` — Private Letter Rulings
- `/page/otr-declaratory-orders` — Declaratory Orders
- `/page/notices-regarding-combined-reporting` — Combined Reporting Notices

Each page lists individual documents either as a direct born-digital PDF link
(under `/sites/default/files/.../attachments/`) or as a `/node/{id}` link. Each
node page renders an `<h1>` title and embeds the document's PDF under
`/attachments/`. The scraper walks every category page, collects both direct-PDF
and node links, resolves each node to its embedded PDF, downloads it, and
extracts full text with `common.pdf_extract` (pdfplumber). The publication date
is parsed from the document body (or the title as a fallback). No JavaScript, no
CAPTCHA, no auth.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # Fetch ~12 sample docs
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Tax notices, rulings and declaratory orders of the DC Office of Tax and Revenue are official government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
