# US/OK-TaxDecisions — Oklahoma Tax Commission, Commission Decisions

Full text of the **Oklahoma Tax Commission's Commission Decisions** — final
determinations by the Tax Commissioners in an adversarial hearing on a
taxpayer's tax protest or claim (`case_law`). Decisions are classified
**Precedential** (relied upon prospectively by the Commission and the public)
or **Non-precedential**; both are published as public, taxpayer-identity-
redacted, **born-digital PDFs** on `oklahoma.gov` (~1,800 decisions).

Each decision is captioned "(PRECEDENTIAL) DECISION OKLAHOMA TAX COMMISSION"
with a structured header (JURISDICTION, CITE, ID, DATE, DISPOSITION, TAX TYPE,
APPEAL) followed by the ORDER body.

## Access

No JavaScript, no CAPTCHA, no auth. The Commission Decisions page (an Adobe
Experience Manager site) is backed by a **master CSV index** whose URL is
exposed in the page's `data-csv-table-api` attribute. Each CSV row carries:

- `TITLE` — the cite/date (`YYYY-MM-DD-NN`, also the PDF filename)
- `CATEGORY` — tax type (Income, Corporate Income, Sales, Electric, …)
- `PRECEDENTIAL` — `Precedential` or blank
- `DOWNLOAD` — an Excel `=DOWNLOAD("/content/dam/…/<file>.pdf")` formula
  wrapping the decision PDF's absolute path
- `Keyword`

The scraper reads the CSV URL from the hub (falling back to the last-known
path), unwraps each PDF path, downloads it, and extracts the text via the
shared OOM-hardened `common.pdf_extract` helper.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # Full pull (~1,800 decisions)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Notes

- `date` is parsed from the cite's leading `YYYY-MM-DD` (reliable temporal
  key); the same date also appears in the PDF header.
- The CSV is UTF-8 with a BOM, stripped before parsing.
- Type is `case_law`: Commission Decisions are final adjudications of a
  contested taxpayer protest/claim.

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Commission Decisions of the Oklahoma Tax Commission are official state-government adjudicative works, published for public inspection after redaction of taxpayer identity, in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
