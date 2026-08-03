# US/CO-EthicsOpinions — Colorado Independent Ethics Commission

Advisory Opinions, Letter Rulings and Position Statements of the **Colorado
Independent Ethics Commission (IEC)** — the constitutional body created by
Article XXIX of the Colorado Constitution ("Amendment 41") to interpret and
enforce the standards of conduct for Colorado state and local public officials
and employees.

## What this source collects

The IEC publishes three classes of ethics guidance, each being the Commission's
written interpretation of Article XXIX and Title 24, Article 18.5, C.R.S.
(**doctrine**):

| Code | Class | Directed to |
|------|-------|-------------|
| `AO` | Advisory Opinion | A covered public officer / legislator / local official / government employee |
| `LR` | Letter Ruling | A person or entity who is **not** a covered public official |
| `PS` | Position Statement | General policy guidance to officials and the public |

Corpus: ~170 documents, 2008–present. Full text is captured from the
born-digital PDFs (clean text layer — no OCR needed for the modern corpus).

## How it works

- The IEC site is Drupal. Opinions are enumerated on per-year listing pages
  `https://iec.colorado.gov/opinions/iec-opinions-{YEAR}` (2008–present).
- Each listing anchor points to a PDF under
  `https://iec.colorado.gov/sites/iec/files/documents/{CODE} FR.pdf`
  (e.g. `AO 23-01 FR.pdf`), and its link text is a descriptive caption
  (`Advisory Opinion 23-01: Acceptance of Gifts`).
- The scraper walks every year page newest-first, dedups by document code,
  downloads each PDF and extracts the text via the shared
  `common.pdf_extract._extract` backend chain.
- Issue date is parsed from the first `Month DD, YYYY` in the body, falling
  back to Jan 1 of the code year.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain — Colorado state government edict / public record](https://www.law.cornell.edu/uscode/text/17/105) — the opinions, rulings and position statements of the Colorado Independent Ethics Commission are official public records of a Colorado constitutional state agency interpreting the constitution and statute (government-edict works). The IEC's Rules of Procedure require their publication (C.R.S. 24-18.5-101). No attribution required; commercial use permitted.
