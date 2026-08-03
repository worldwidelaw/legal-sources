# US/VA-TaxRulings — Virginia Department of Taxation (Rulings of the Tax Commissioner & Tax Bulletins)

Full text of Virginia Tax's published interpretive guidance — the
Virginia Department of Taxation's official position on how Virginia tax
law applies. Two document families are collected, both **doctrine**:

- **Rulings of the Tax Commissioner** — the Commissioner's written
  determinations on appeals, refund claims and ruling requests under
  Va. Code § 58.1-1821 and related provisions (1980s–present).
- **Tax Bulletins** — general guidance announcements (interest-rate
  changes, federal conformity, filing relief, new legislation).

## Source

- Library: <https://www.tax.virginia.gov/laws-rules-decisions>
- Browse listing: `https://www.tax.virginia.gov/laws-rules-decisions/browse?document_type=<ID>&page=<N>`
  - `document_type=70` → Rulings of the Tax Commissioner
  - `document_type=71` → Tax Bulletins
- Document page (full text): e.g. `https://www.tax.virginia.gov/laws-rules-decisions/rulings-tax-commissioner/07-68`

## How it works

The library is a server-rendered Drupal site. The browse listing is a
25-row table; the scraper walks `?page=N` per document type until a page
yields no rows, reading the document number, public document number,
type, **date issued (MM/DD/YYYY)** and description from each row's
`views-field` cells. Each document page renders the full ruling/bulletin
body as HTML inside an `<article>` element (legacy `<font>`-tagged letter
text), which the scraper extracts, strips of tags, and decodes. No
JavaScript, no CAPTCHA, no auth, no PDF.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Rulings of the Tax Commissioner and Tax Bulletins of the Virginia Department of Taxation are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
