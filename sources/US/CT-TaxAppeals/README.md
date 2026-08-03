# US/CT-TaxAppeals — Connecticut Superior Court, Tax and Administrative Appeals Session

Full text of the published decisions of the **Tax and Administrative Appeals
Session** of the Connecticut Superior Court — the specialized session (sitting
at the judicial district of New Britain) that hears municipal property-tax
appeals (Conn. Gen. Stat. §§ 12-117a / 12-119), state tax appeals against the
Commissioner of Revenue Services, and related administrative appeals. Each
"Memorandum of Decision" resolves a specific tax controversy
(taxpayer v. town / Commissioner of Revenue Services), so the corpus is
**case_law**.

## Source

- **Index:** https://www.jud.ct.gov/external/super/Tax/recent.htm
- **PDFs:** served from `https://info.jud.ct.gov/external/super/Tax/Decisions/`
  (the `www` host 302-redirects the PDF path to the `info` content host).

The index is a single static HTML page grouping every published decision by
year (`<h2>` year headers, 2001–2016) as `<li>` entries carrying the case
caption, court, docket number, decision date and judge, each linking to a
born-digital text-layer PDF. No JavaScript, no CAPTCHA, no authentication.

## Method

1. `GET recent.htm` and parse each `<li>` for its PDF href, caption, docket,
   decision date and judge.
2. Download each PDF from `info.jud.ct.gov` and extract its text layer via
   `common.pdf_extract`.
3. Normalize into the standard `case_law` schema (`text` = full decision body).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample decisions
python bootstrap.py bootstrap            # Full pull (all decisions)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Connecticut Superior Court are official judicial government works in the public domain under the government-edicts doctrine. No attribution required; commercial use permitted.
