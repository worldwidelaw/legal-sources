# US/GA-TaxTribunal — Georgia Tax Tribunal (Decisions & Orders)

Full text of the decisions and orders of the **Georgia Tax Tribunal**, an
independent quasi-judicial tribunal created by the Georgia Tax Tribunal Act
of 2012 (operative January 1, 2013). The Tribunal hears appeals of tax
matters arising under the laws administered by the Georgia Department of
Revenue — income tax, sales and use tax, property-tax digest disputes,
motor-fuel tax, and penalty/refund controversies — between taxpayers and the
Commissioner of the Georgia Department of Revenue. Each "Decision" or "Order"
resolves a specific tax controversy, so the corpus is **case_law**.

## Source

- **Listing:** https://gataxtribunal.georgia.gov/decisions
- Server-rendered Drupal listing, paginated 10 per page via `?page=N`
  (page 0 is the first page).
- Each row links a born-digital text-layer PDF at
  `/document/decisions/{slug}/download` and carries a `data-text` caption of
  the form `<parties> - <doc type>, <YYYY-N> Ga. Tax Tribunal, <Month D, YYYY>`.
- No JavaScript, no CAPTCHA, no auth. ~80 decisions (2014–present).

## How it works

1. `discover_documents()` walks the paginated `/decisions` listing until a
   page yields no new rows, parsing each row's PDF href and `data-text`
   caption (parties, citation, decision date).
2. `_build_raw()` downloads each PDF (curl, browser UA, ~1 req/s) and
   extracts the text layer via `common.pdf_extract`.
3. `normalize()` maps to the standard `case_law` schema with full `text`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample decisions -> sample/
python bootstrap.py bootstrap            # full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Georgia tribunal decision)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Georgia Tax Tribunal are official quasi-judicial government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
