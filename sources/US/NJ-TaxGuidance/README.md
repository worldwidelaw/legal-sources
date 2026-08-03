# US/NJ-TaxGuidance — New Jersey Division of Taxation, Technical Bulletins & Letter Rulings

Full text of the **New Jersey Division of Taxation**'s official published
interpretive tax guidance (`doctrine`):

- **Technical Bulletins (TBs)** — the Division's published interpretation of the
  law, regulations, and policy on a given tax topic (~89 documents).
- **Letter Rulings (LRs)** — written determinations issued on behalf of the
  Director applying the law/regulations/policy to a specific taxpayer's facts.
  An LR binds the Division only as to its recipient, but is published as guidance
  for like-situated taxpayers (~23 documents).

This is guidance/doctrine, **not** adjudication. NJ Tax Court case law is covered
by `US/NJ-Courts`; NJ statutes by `US/NJ-Legislation`.

## Source

- Technical Bulletins index: <https://www.nj.gov/treasury/taxation/tech-pubs.shtml>
- Letter Rulings index: <https://www.nj.gov/treasury/taxation/letterrulings-pubs.shtml>
- PDFs: `https://www.nj.gov/treasury/taxation/pdf/pubs/{tb,letter_rulings}/*.pdf`

## Method

1. Fetch both server-rendered index pages (uniform 5-column tables: doc number +
   PDF link, title, tax type, `YYYY Mon` issue date).
2. For each row carrying a `/pubs/` PDF link, resolve the PDF URL and capture the
   document number, title, tax type, and issue date.
3. Download each born-digital PDF and extract text via the shared
   `common.pdf_extract` helper (pdfplumber → pypdf; no OCR needed).

No authentication, no CAPTCHA. Rate limited to ~1 request/second.

## Fields

`_id`, `_source`, `_type` (`doctrine`), `_fetched_at`, `doc_no`, `kind`
(Technical Bulletin | Letter Ruling), `tax_type`, `issuer`, `title`, `text`
(full document text), `url`, `date` (issue date, ISO 8601), `jurisdiction`
(`US-NJ`).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105 (US government edicts)](https://www.law.cornell.edu/uscode/text/17/105) — Technical Bulletins and Letter Rulings of the New Jersey Division of Taxation are official New Jersey state-government works, published for public inspection, in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
