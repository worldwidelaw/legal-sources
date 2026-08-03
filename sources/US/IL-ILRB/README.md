# US/IL-ILRB — Illinois Labor Relations Board (Decisions & Orders)

Full text of the published decisions of the **Illinois Labor Relations Board
(ILRB)**, the state's quasi-judicial agency that adjudicates public-sector
labor-relations disputes under the **Illinois Public Labor Relations Act
(5 ILCS 315)**.

The Board — sitting as the **State Panel** and the **Local Panel** — together
with its Administrative Law Judges and Executive Director / General Counsel,
decides unfair-labor-practice charges (CA/CB), representation petitions (RC/RD),
unit-clarification petitions (UC), and related contested cases. Each decision
resolves a specific case and is `case_law`.

- **Coverage:** Board Decisions and Orders + ALJ / Executive Director
  Recommended Decisions and Orders, paginated by state fiscal year
  (FY12–present).
- **Jurisdiction:** US-IL
- **Type:** `case_law`

## Source & method

Decisions are published on the Illinois.gov Adobe Experience Manager (AEM) site
at <https://ilrb.illinois.gov/decisions.html>. Two families are paginated by
fiscal year:

- Board Decisions and Orders — `/decisions/boarddecisions/boardfyNN.html`
- ALJ / ED Recommended Decisions and Orders — `/decisions/decisionorders/decisionordersfyNN.html`

Each fiscal-year page renders a JavaScript DataTable whose rows are served by an
AEM "datatableassets" JSON endpoint embedded in the page HTML
(`…/jcr:content/…/data_table_assets_co*.datatableassets.json`). Each row is:

```
[ [caseNumber, pdfRelPath, "true"], dateIssued, periCitation, parties, documentCategory ]
```

The `pdfRelPath` (under `/content/dam/...`) is the born-digital decision PDF.
The scraper walks every fiscal-year page of both categories, reads the embedded
JSON tables, downloads each PDF, and extracts the full text with the shared
`common.pdf_extract` extractor. No auth, no CAPTCHA.

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # Full pull
python bootstrap.py bootstrap-fast      # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Illinois Labor Relations Board are official works of Illinois state government (edicts of a quasi-judicial government body) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially.
