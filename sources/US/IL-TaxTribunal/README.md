# US/IL-TaxTribunal — Illinois Independent Tax Tribunal (Decisions & Rulings)

Full text of every issued decision / ruling of the **Illinois
Independent Tax Tribunal**, the executive-branch tribunal created by the
Illinois Independent Tax Tribunal Act of 2012. It adjudicates disputes
between taxpayers and the **Illinois Department of Revenue** over notices
of tax liability, penalties and refund denials (income, sales/use,
excise, etc.). Each document resolves a tax controversy, so the corpus is
**case_law**.

## Source & access

The Tribunal publishes its final decisions on its **Decisions/Rulings**
page (`taxtribunal.illinois.gov/decisions-rulings.html`). That page is an
Adobe AEM site whose decision table is loaded client-side from a JSON
endpoint:

```
/content/soi/taxtribunal/en/decisions-rulings/jcr:content/responsivegrid/container/data_table.datatablejson.json
```

The JSON returns one row per decision: the case number (hyperlinked to
the decision PDF under `/content/dam/.../rules-decisions/`), year,
decision date, case caption and subject. The PDFs are born-digital
text-layer documents. The scraper fetches the JSON directly (no browser
needed), then downloads and extracts each PDF via `common.pdf_extract`.

Small but complete corpus (~33 published final decisions, 2014–present).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample decisions
python bootstrap.py bootstrap           # full pull (all decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Illinois)](https://www.law.cornell.edu/uscode/text/17/105) — Illinois Independent Tax Tribunal decisions are official state government works in the public domain. Commercial use OK, no attribution required.
