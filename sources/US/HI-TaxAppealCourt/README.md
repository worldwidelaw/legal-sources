# US/HI-TaxAppealCourt — Hawaii Tax Appeal Court (Unreported Decisions)

Full text of the redacted, unreported decisions of the **Hawaii Tax
Appeal Court**, the specialized Hawaii state trial court that hears
appeals from the Department of Taxation and the Board of Review under
[HRS chapter 232](https://www.capitol.hawaii.gov/hrscurrent/Vol04_Ch0201-0257/HRS0232/).
Cases cover general excise, income, use, conveyance and other Hawaii
taxes.

- **Type:** `case_law` (adjudications of specific contested tax appeals)
- **Publisher:** Hawaii Department of Taxation
- **Index:** https://tax.hawaii.gov/legal/a4_5crtcases/
- **Documents:** born-digital text-layer PDFs on `files.hawaii.gov`
- **Coverage:** currently 1968–1997 (the published "unreported decisions" set)

## How it works

The index page is a set of server-rendered HTML tables sharing one
column layout: `Date | Case No.(s) | Tax Law | In the Matter of the Tax
Appeal of`. Rows whose party cell links to a PDF are the retrievable
decisions; index-only rows without a document are skipped. Each PDF is
downloaded and its text layer extracted via `common.pdf_extract`
(~1 req/s, browser UA). No JavaScript, CAPTCHA or authentication.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## Relationship to other Hawaii sources

- **US/HI-TaxGuidance** — TIRs & Letter Rulings (interpretive `doctrine`)
- **US/HI-Courts** — general Hawaii appellate judiciary
- **US/HI-AGOpinions** — Attorney General opinions (`doctrine`)
- **US/HI-Legislation** — Hawaii Revised Statutes

This source fills the gap for Hawaii's specialized **tax trial court**
adjudications.

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Hawaii Tax Appeal Court are official state-government works in the public domain under the government-edicts doctrine. No attribution required; commercial use permitted.
