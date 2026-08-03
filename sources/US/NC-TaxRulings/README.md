# US/NC-TaxRulings — North Carolina Department of Revenue, Written Determinations

Full text of the interpretive **written determinations** published by the
North Carolina Department of Revenue (NCDOR):

- **Corporate Tax Private Letter Rulings** (`CPLR YYYY-NN`) and
  **Redetermination Letters** — corporate/franchise (and insurance
  gross-premium) tax.
- **Sales & Use Tax Private Letter Rulings** (`SUPLR YYYY-NNNN`).
- **Personal Tax Private Letter Rulings** (`PTPLR YYYY-N`).

A *written determination* applies North Carolina tax law to a specific set of
facts furnished by a particular taxpayer; NCDOR publishes each one as a public,
taxpayer-identifier-**redacted** PDF. These are official state-government
interpretive guidance (not adjudications of a contested case), so the corpus is
typed `doctrine`.

## Source

Each tax division has a server-rendered Drupal listing page (no JavaScript, no
CAPTCHA, no auth):

- Corporate: `…/written-determinations/written-determinations-corporate-tax/written-determinations-corporate-tax-0`
- Sales & Use: `…/other-sales-and-use-tax-resources/written-determinations-sales-and-use-tax`
- Personal: `https://www.ncdor.gov/written-determinations-personal-taxes`

Each listing anchor carries the determination number and links to the
document's node page (or a direct download). The full text lives only in the
attached **born-digital PDF**, reached via a `…/<slug>/open` or `…/download`
link and extracted with the shared `common.pdf_extract` helper.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — written determinations (private letter rulings, redetermination letters) of the North Carolina Department of Revenue are official US state-government works in the public domain under the government-edicts doctrine. Commercial use permitted.
