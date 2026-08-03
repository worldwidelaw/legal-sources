# US/SC-AdvisoryOpinions — South Carolina Department of Revenue (Advisory Opinions)

Full text of every published advisory opinion of the **South Carolina
Department of Revenue (SCDOR)** — the agency's official interpretive
guidance on South Carolina tax law. Advisory opinions comprise:

- **Revenue Rulings (RR)** — the Department's official advisory opinion
  on how the law applies to a specific set of facts.
- **Revenue Procedures (RP)** — procedural / management practice
  statements.
- **Private Letter Rulings (PLR)** — written advice to a specific
  taxpayer on a specific transaction.
- **Information Letters (IL)** — general informational announcements.
- **Technical Advice Memoranda (TAM)**

All are official state-government interpretive guidance, so the corpus is
classified as **doctrine** (distinct from `US/SC-Courts` judicial
decisions, `US/SC-Legislation` statutes and `US/SC-AGOpinions` Attorney
General opinions).

## Data type

`doctrine`

## Source / access

The SCDOR [Advisory Opinion Search](https://dor.sc.gov/advisory-opinion-search)
is a Drupal Views page whose exposed filters accept GET parameters and
render a server-side results table, paginated 10 rows per page via
`?page=N`. Each row gives the policy name, **Policy #** (e.g. `RR03-4`),
tax category, year, opinion type and status. Every opinion's PDF lives at
a deterministic URL:

```
https://dor.sc.gov/sites/dor/files/policies/{PolicyID}.pdf
```

so no per-row node fetch is needed. PDFs are born-digital text-layer
documents (owner-password encrypted but extractable). ~1,888 opinions,
1987–present. No JavaScript, no CAPTCHA, no auth.

PDFs are extracted via `common.pdf_extract` (curl browser UA, ~1 req/s).
A `<150`-char guard auto-skips the occasional scanned attachment with no
text layer (recoverable on an OCR host).

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample opinions
python bootstrap.py bootstrap           # Full pull (streams to data/records.jsonl)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Advisory opinions of the South Carolina Department of Revenue are
official state-government works in the public domain under the
government-edicts doctrine. Commercial use permitted, no attribution
required.
