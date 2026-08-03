# US/IL-LetterRulings — Illinois Department of Revenue Letter Rulings

Full text of the Illinois Department of Revenue's published interpretive
guidance: **Private Letter Rulings (PLR)** — binding determinations issued to
the requesting taxpayer on the specific facts presented — and **General
Information Letters (GIL)** — non-binding general statements of the
Department's interpretation. Covers **income tax** and **sales/use tax**,
**2010–present**.

These are official state-government interpretive guidance (`doctrine`), not
adjudications of a contested case. Contested taxpayer disputes are decided by
the Illinois Independent Tax Tribunal — see **US/IL-TaxTribunal** (`case_law`).

## Source

- Library index: https://taxarchive.illinois.gov/research/legal/letter-rulings.html
- Income tax / sales tax sections, partitioned by year:
  `/research/legal/letter-rulings/{income-tax|sales-tax}/{YEAR}.html`
- Each ruling PDF: `/content/dam/soi/en/web/taxarchive/research/legal/letter-rulings/{tax-type}/{YEAR}/{DOCID}.pdf`
  where `DOCID` is e.g. `IT24-0001-PLR` (income tax) or `ST24-0007-GIL` (sales tax).

## How it works

1. For each tax family and year, fetch the server-rendered index page and
   regex the dam PDF hrefs.
2. Download each PDF and extract its text layer via `common.pdf_extract`
   (born-digital, clean text layer).
3. Parse the issue date from the document head (`M/D/YYYY`), falling back to
   the partition year, and normalize into the standard `doctrine` schema.

No JavaScript, no CAPTCHA, no authentication. ~1 request/second.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction check
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — letter rulings of the Illinois Department of Revenue are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
