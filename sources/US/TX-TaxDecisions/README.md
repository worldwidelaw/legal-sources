# US/TX-TaxDecisions — Texas Comptroller STAR (State Tax Automated Research)

Full text of the Texas Comptroller of Public Accounts' **STAR** database —
the state's official public tax-research library.

- **Publisher:** Texas Comptroller of Public Accounts
- **Site:** https://star.comptroller.texas.gov/
- **API:** https://api.comptroller.texas.gov/star/v1 (no auth, no CAPTCHA)
- **Corpus:** ~24,700 documents, back to the 1980s
- **Types captured:**
  - **Hearings** (`H`) → `case_law` — redacted administrative adjudications of
    taxpayer protests (Comptroller / SOAH decisions)
  - **Court Cases** (`C`) → `case_law` — summaries of tax litigation
  - **Rules** (`R`) → `legislation` — adopted Comptroller administrative rules
    (34 TAC) with preamble
  - **Letters / Memos / Publications / Web Content** (`L`/`M`/`P`/`W`) →
    `doctrine` — letter rulings, policy memos, tax publications, guidance

## How it works

1. **Discovery** — `GET /search?q=&date_range=YYYY-01-01,YYYY-12-31&limit=5000`
   for each year. The corpus is partitioned by year because the API's `start`
   offset is not honored and large `limit` values time out; each year returns
   only a few hundred rows, so one call retrieves the year in full. Dedup on
   `acc_no`.
2. **Full text** — `GET /view/{acc_no}` returns `data.contents` (HTML); tags
   are stripped and entities unescaped to clean plain text. A 200-char guard
   skips rows without a real body.

Taxpayer-identifying details are redacted at source (asterisks).

## Usage

```bash
python bootstrap.py test-api            # connectivity + one extraction
python bootstrap.py bootstrap --sample  # ~12 mixed-type samples (from 2024)
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain — 17 U.S.C. § 105 / edicts of government](https://www.law.cornell.edu/uscode/text/17/105)
— STAR documents are official works of the Texas Comptroller of Public
Accounts published for public inspection (administrative hearings, court-case
summaries, adopted rules, and public agency guidance). Public domain,
commercial use permitted.
