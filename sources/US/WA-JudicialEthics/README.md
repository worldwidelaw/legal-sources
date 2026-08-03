# US/WA-JudicialEthics — Washington State Ethics Advisory Committee (Judicial Ethics Advisory Opinions)

Full text of the **Ethics Advisory Opinions** issued by the **Washington State
Ethics Advisory Committee (EAC)**, a committee established by the Washington
Supreme Court to advise judges and judicial officers on the **Washington Code of
Judicial Conduct (CJC)**.

Each opinion answers a specific inquiry, states the applicable CJC rules/canons,
and gives the Committee's conclusion. These are the Committee's official written
interpretations of the judicial-conduct rules = **doctrine**.

This is **distinct from `US/WA-EthicsOpinions`**, which covers the Washington
State **Executive** Ethics Board (state executive-branch employees). This source
covers the **judicial** branch.

## Coverage

- ~468 opinions, from **1984 (84-01)** to present.
- Born-digital HTML full text (no OCR, no PDF, no CAPTCHA, no auth).

## Access

1. **Index** — `?fa=pos_ethics.byyear` lists every opinion as a
   `mode=NNNN` link, where `NNNN` is a four-digit `YYSS` code (two-digit year +
   two-digit sequence), e.g. `mode=2001` → Opinion 20-01.
2. **Opinion page** — `?fa=pos_ethics.dispopin&mode=NNNN` renders the opinion.
   The body sits between an `Opinion YY-SS` header and a footer that repeats
   `Opinion YY-SS`, the issue date (`MM/DD/YYYY`), an optional
   `Amended MM/DD/YYYY` line, and the site nav (`RECORDS`). The scraper slices
   out the body and de-tags it.

> `courts.wa.gov` returns HTTP 403 to non-browser User-Agents; all requests
> carry a desktop-Chrome UA.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain (Washington State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Ethics Advisory
Opinions of the Washington State Ethics Advisory Committee are official public
records of the State of Washington, published for public use with no copyright
restriction. Commercial use permitted; no attribution required.
