# US/VA-JudicialEthics — Virginia Judicial Ethics Advisory Committee (JEAC) Opinions

Full-text advisory opinions of the **Virginia Judicial Ethics Advisory Committee
(JEAC)**, a committee established by Order of the Supreme Court of Virginia that
issues advisory opinions on whether a judge's proposed future conduct complies
with the **Canons of Judicial Conduct** for the State of Virginia → **doctrine**.

- **Publisher:** Supreme Court of Virginia — Judicial Ethics Advisory Committee
- **Coverage:** ~45 opinions, 1999–present, numbered `YY-N`
- **Source:** <https://www.vacourts.gov/programs/jeac/opinions/home> (single public index)
- **Format:** clean HTML pages (1999–2008) + born-digital PDFs (2016–present), no OCR
- **Type:** `doctrine`

Distinct from **US/VA-EthicsOpinions** (the Virginia Conflict of Interest &
Ethics Advisory Council, which advises public officials) and **US/VA-LegalEthics**
(Virginia State Bar Legal Ethics Opinions advising lawyers). This source is the
**judicial-branch** committee advising **judges** under the Canons of Judicial
Conduct.

## How it works

1. GET the single index page. Each opinion is one anchor whose **text** is the
   opinion number (e.g. `99-7`, `24-2`) and whose href is taken verbatim.
2. Two formats: older opinions (1999–2008) are HTML pages
   (`/programs/jeac/opinions/{YYYY}/{num}`); newer opinions (2016–present) are
   born-digital PDFs (`/static/programs/jeac/opinions/{YYYY}/{num}.pdf`).
3. HTML body is read from `<main>`; PDFs are extracted with PyMuPDF (`fitz`).
4. Date comes from the `Date Issued:` line (HTML) or the transmittal
   `the Nth day of Month, YYYY` (PDF), falling back to the opinion-number year.

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain (Virginia State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Virginia judicial ethics advisory opinions are official public records of the Supreme Court of Virginia, published on vacourts.gov for public use with no copyright restriction. Commercial use permitted.
