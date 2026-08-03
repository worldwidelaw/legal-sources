# US/NY-EthicsOpinions — New York State Commission on Ethics and Lobbying in Government

Formal **Advisory Opinions** of the New York State ethics regulator — the
**Commission on Ethics and Lobbying in Government (COELIG)** and its
predecessors — interpreting the Public Officers Law (§§ 73, 73-a, 74), the
Legislative Law and the ethics/lobbying statutes they administer
(**doctrine**).

## Coverage

The corpus (~180 opinions, **1988–present**) spans the successive NY ethics
regulators:

| Body | Years |
|------|-------|
| Commission on Ethics and Lobbying in Government (COELIG) | 2022–present |
| Joint Commission on Public Ethics (JCOPE) | 2011–2022 |
| Commission on Public Integrity | 2007–2011 |
| NYS Ethics Commission / Temporary State Commission on Lobbying | 1988–2007 |

Full text is captured from the born-digital opinion PDFs (clean text layer;
OCR / inline-HTML fallback for the oldest opinions).

## How it works

- `ethics.ny.gov` is Drupal. The advisory-opinions Views listing
  `https://ethics.ny.gov/ethics-advisory-opinions?page=N` (N = 0..~35)
  enumerates every opinion as a node link (`Advisory Opinion No. 25-03`).
- Each opinion node page has a subject/summary and a **Download** link that
  redirects to the full PDF at
  `/system/files/documents/YYYY/MM/advisory-opinion-{code}.pdf`.
- The scraper walks all listing pages, dedups by node slug, fetches each node,
  follows the Download link and extracts text via
  `common.pdf_extract._extract`. Issue date is parsed from the first
  `Month DD, YYYY` in the body, falling back to Jan 1 of the opinion-number
  year (2-digit year: 88–99 → 19xx, 00–87 → 20xx).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # Fetch ~12 samples (newest first)
python bootstrap.py bootstrap            # Full pull (all opinions)
```

This source is distinct from **US/NY-AGOpinions** (Attorney General opinions),
which has a different issuer and statute base.

## License

[Public Domain — New York state government edict / public record](https://www.law.cornell.edu/uscode/text/17/105) — formal advisory opinions of the New York State ethics regulator are official public records of a New York State agency interpreting statute (government-edict works), published and by statute made public. No attribution required; commercial use permitted.
