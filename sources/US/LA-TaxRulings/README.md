# US/LA-TaxRulings — Louisiana Department of Revenue Policy Documents

Full text of the interpretive **Policy Documents** published by the Louisiana
Department of Revenue (LDR), Policy Services Division:

- **Revenue Rulings** (`RR YY-NNN`) — the Department's formal written
  interpretation of how Louisiana tax law applies to a stated set of facts.
- **Private Letter Rulings** (`PLR YY-NNN`, redacted) — written
  determinations issued to a specific taxpayer, published in redacted form.
- **Revenue Information Bulletins** (`RIB YY-NNN`) — announcements and
  explanatory statements of Department policy.
- **Statements of Acquiescence / Nonacquiescence**, **Remote Sellers
  Information Bulletins**, and other **Guidance Documents**.

All are official state-government interpretive guidance (not adjudications of
a contested case), so the corpus is classified as `doctrine`.

## Source

- Index: <https://revenue.louisiana.gov/tax-policy/policies>
- Document PDFs: `https://dam.ldr.la.gov/lawspolicies/<file>.pdf`

The Policies page is a single server-rendered list. Each document is a
`<div class="card download-card filterable {policy_type} {tax_type} {year}">`
carrying the title, the policy number, the issue date, and an anchor to the
public PDF on the LDR digital-asset host. Full text lives only in the PDF and
is extracted via the shared `common.pdf_extract` helper (pdfplumber → pypdf →
OCR fallback). No CAPTCHA, no JavaScript challenge, no auth.

A handful of older files 404 on the asset host and are skipped. The corpus is
~840 documents (the large majority Revenue Information Bulletins, plus ~136
Revenue Rulings and ~51 redacted Private Letter Rulings).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction smoke test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~840 documents)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Revenue Rulings, Private Letter Rulings, Revenue Information Bulletins and
other policy documents of the Louisiana Department of Revenue are official
state-government works in the public domain under the government-edicts
doctrine. Commercial use permitted; no attribution required.
