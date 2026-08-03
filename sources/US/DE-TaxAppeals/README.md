# US/DE-TaxAppeals — Delaware State Tax Appeal Board (Opinions)

Full text of the **Delaware State Tax Appeal Board**'s published opinions
(Decisions & Orders). The Board (29 Del. C. § 8306; 30 Del. C. ch. 3
subch. II) is the independent state tribunal that hears appeals from
determinations of the Delaware Division of Revenue (personal income,
corporate income, franchise, gross-receipts and other state taxes) and from
the State Escheator's unclaimed-property determinations. Every published
opinion adjudicates a specific contested case → **case_law**.

## Source

- **Index:** <https://finance.delaware.gov/state-tax-appeal-board/opinions-of-the-tax-appeal-board/>
- **PDFs:** `https://financefiles.delaware.gov/TAB/{filename}.pdf`
- One server-rendered page lists ~237 opinions by docket number. No
  JavaScript, no CAPTCHA, no auth.
- The filename encodes the docket number(s) and usually the party name
  and/or the decision date (e.g. `1815 Parsons TAB Decision and Order dated
  10.28.2024.pdf`, `422 423 424 Heisler.pdf`).

## Full text / OCR

The opinions are **scanned images** with no embedded text layer (both the
oldest 1971 opinions and the newest 2024/2025 ones). Full text is read via
the OCR fallback in `common.pdf_extract` (PyMuPDF → `pytesseract`), which
requires the **`tesseract`** binary to be installed on the host. Budget for
OCR slowness across the ~237-opinion corpus.

## Usage

```bash
python bootstrap.py test-api           # connectivity + OCR smoke test
python bootstrap.py bootstrap --sample # ~12 sample records
python bootstrap.py bootstrap          # full pull
```

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
opinions of a U.S. state administrative tribunal are public-domain government
edicts. Commercial use permitted.
