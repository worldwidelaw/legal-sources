# US/PA-BFR — Pennsylvania Board of Finance and Revenue (Decisions)

Full text of the published decisions of the **Pennsylvania Board of Finance
and Revenue (BF&R)** — the Commonwealth's independent administrative
tax-appeal tribunal that reviews petitions for refund/reassessment of state
taxes (Personal Income; Corporate, Franchise & Capital Stock; Sales & Use;
Realty Transfer; and other "Miscellaneous" taxes) appealed from the Department
of Revenue's Board of Appeals. Each redacted "Decision and Order" resolves a
specific petition (taxpayer v. Commonwealth Dept. of Revenue), so the corpus
is **case_law**. Decisions are published since April 1, 2014.

## Source

- **Search app:** https://bfrcases.patreasury.gov/DecisionSearch
- **Enumeration:** `POST /DecisionSearch.aspx/SearchDocket` with body
  `{"Docket":""}` returns the whole published corpus (~1,000 rows — the stored
  procedure does a SQL `LIKE '%%'` match; `TransactionId`s are contiguous
  `10..1014`, so this is the full set, not a paginated cap).
- **Documents:** `OpenDocument.aspx?id={TransactionId}&fname={FileName}`
  (URL-encode the FileName) → born-digital text-layer PDF.

No JavaScript, no CAPTCHA, no authentication.

## Method

1. `POST SearchDocket {"Docket":""}` → parse every row for `TransactionId`,
   `DocketNumber`, `PetitionerName`, `TaxName`, `SubTaxName`, `FileName`.
2. Download each decision PDF and extract its text layer via
   `common.pdf_extract`.
3. Parse the decision date from the standard ordering clause
   `AND NOW, <Month D, YYYY>, pursuant to the Fiscal Code ...` (fallback:
   latest in-document date). Normalize into the `case_law` schema.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample decisions
python bootstrap.py bootstrap            # Full pull (~1,000 decisions)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Pennsylvania Board of Finance and Revenue are official quasi-judicial government works in the public domain under the government-edicts doctrine. Taxpayer-identifying details are redacted by the Board prior to publication. No attribution required; commercial use permitted.
