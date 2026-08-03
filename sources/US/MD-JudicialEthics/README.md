# US/MD-JudicialEthics — Maryland Judicial Ethics Committee Opinions

Full-text advisory opinions of the **Maryland Judicial Ethics Committee**, a
committee of the Maryland Judiciary (Maryland Rules, Title 18) that renders
written advisory opinions to inquiring Maryland judges and judicial appointees
on the propriety of contemplated conduct under the Maryland **Code of Judicial
Conduct** and the Code of Conduct for Judicial Appointees. Each opinion
interprets the judicial-conduct rules for the requesting judge → **doctrine**.

- **Publisher:** Maryland Judiciary — Judicial Ethics Committee
- **Coverage:** ~257 published opinions, 1971–present, numbered `YYYY-NN`
- **Source:** <https://www.mdcourts.gov/ethics/opinions> (single public index page)
- **Format:** born-digital PDFs (text layer) — extracted with PyMuPDF, no OCR
- **Type:** `doctrine`

Distinct from **US/MD-EthicsOpinions** (the executive Maryland State Ethics
Commission, which advises public officials/employees) and **US/MD-LegalEthics**
(bar/lawyer professional-responsibility ethics). This source is the
**judicial-branch** committee advising **judges** under the Code of Judicial
Conduct.

## How it works

1. GET the single index page. Each opinion is one HTML table row:
   `<a href="/sites/default/files/import/ethics/pdfs/{file}.pdf">{number}</a>`
   `| {MM-DD-YY issue date} | {subject}`.
2. The anchor **text** is the authoritative opinion number (the PDF filename is
   sometimes typo'd or uses underscores), so we take the href verbatim to
   download and the anchor text as the number.
3. Download each PDF and extract the full text with PyMuPDF (`fitz`).
4. Date comes from the index table's `MM-DD-YY` column.

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (Maryland State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Maryland judicial ethics opinions are official public records of the Maryland Judiciary, published on mdcourts.gov for public use with no copyright restriction. Commercial use permitted.
