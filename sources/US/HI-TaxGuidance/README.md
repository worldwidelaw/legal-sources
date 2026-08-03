# US/HI-TaxGuidance — Hawaii Department of Taxation (Tax Information Releases & Letter Rulings)

Full text of the official tax guidance published by the **Hawaii
Department of Taxation** at [tax.hawaii.gov/legal](https://tax.hawaii.gov/legal/):

- **Tax Information Releases (TIRs)** — the Department's formal published
  interpretations of Hawaii tax law (General Excise Tax, income tax, use
  tax, conveyance tax, withholding, etc.), 1963–present.
- **Letter Rulings** — written determinations applying the tax law to a
  specific taxpayer's facts.

Both are official interpretive government guidance, so the corpus is
classified as **doctrine** (distinct from `US/HI-Courts` judicial
decisions and `US/HI-Legislation` statutes).

## Data type

`doctrine`

## Source / access

The three index pages share one server-rendered HTML table layout —
each row is `[number, issue date, title]` with a link to a born-digital
text-layer PDF hosted on `files.hawaii.gov`:

| Page | URL |
|------|-----|
| Current TIRs | `https://tax.hawaii.gov/legal/tir/` |
| TIR archive (1963–2009) | `https://tax.hawaii.gov/legal/tirarchive/` |
| Letter Rulings | `https://tax.hawaii.gov/legal/letters/` |

No JavaScript, no CAPTCHA, no auth. PDFs are extracted via
`common.pdf_extract` (curl browser UA, ~1 req/s). The occasional scanned
attachment with no text layer is auto-skipped (<150-char guard) and is
recoverable on an OCR host.

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # Full pull (streams to data/records.jsonl)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Tax Information Releases and Letter Rulings of the Hawaii Department of
Taxation are official state-government works in the public domain under
the government-edicts doctrine. Commercial use permitted, no attribution
required.
