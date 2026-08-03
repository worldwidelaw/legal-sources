# US/SC-JudicialEthics — South Carolina Advisory Committee on Standards of Judicial Conduct

Advisory opinions of the **South Carolina Advisory Committee on Standards of
Judicial Conduct**, a committee created by the Supreme Court of South Carolina
(Rule 503, SCACR) to render written advisory opinions to inquiring judges on the
propriety of contemplated judicial and nonjudicial conduct under the South
Carolina Code of Judicial Conduct. This is the **judicial-branch** advisory
committee that advises judges — distinct from `US/SC-EthicsOpinions` (the
executive State Ethics Commission advising public officials/employees).

- **Publisher:** Supreme Court of South Carolina
- **Coverage:** ~500+ opinions, 1989–present (numbered `NN-YYYY`)
- **Type:** `doctrine` (official written interpretation of the judicial-conduct rules)
- **Full text:** born-digital PDF (text layer), extracted with PyMuPDF — no OCR, no CAPTCHA, no auth

## Access

The Committee publishes a per-year index and each opinion as a born-digital PDF:

```
Index:   https://www.sccourts.org/opinions-orders/judicial-advisory-opinions/?year=YYYY
Opinion: https://www.sccourts.org/media/advisoryOpinions/html/{NN}-{YYYY}.pdf
```

The scraper iterates the years 1989..present, collects every opinion PDF link
plus its subject (`p.subtitle` in each `.result` block), downloads each PDF and
extracts the full body text.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## License

[Public Domain (South Carolina State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — South Carolina judicial advisory opinions are official public records of the Supreme Court of South Carolina, published on sccourts.org for public use with no copyright restriction. Commercial use permitted; no attribution required.
