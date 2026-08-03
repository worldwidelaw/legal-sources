# US/PA-PLRB — Pennsylvania Labor Relations Board (Final & Proposed Orders)

Full text of the **Pennsylvania Labor Relations Board (PLRB)** Final Orders and
Proposed Decisions & Orders. PLRB (within the PA Department of Labor & Industry)
adjudicates public- and private-sector labor-relations disputes under the
Public Employe Relations Act (PERA / Act 195), the Police & Firemen Collective
Bargaining Act (Act 111), and the Pennsylvania Labor Relations Act (PLRA) —
unfair-labor-practice charges, representation / certification petitions, and
unit-determination cases. Each order resolves a specific contested case =
**case_law**.

## Source

- **Agency:** https://www.pa.gov/agencies/dli/programs-services/labor-management-relations/pennsylvania-labor-relations-board
- **Final Orders:** `.../plrb-final-orders/{year}-...` (~2011–present)
- **Proposed Orders:** `.../plrb-proposed-orders/{year}-...` (~2020–present)

## How it works

PLRB migrated to the Commonwealth AEM site (`www.pa.gov`). Orders are published
on per-year pages, but the per-year URL slugs are **inconsistent** (e.g.
`2024-final-orders` vs `2023-plrb-final-orders`; a typo `{year}-plrb-proposed-urders`
for 2020–2022). The landing-page year navigation is JS-rendered and AEM
`.model.json` is blocked, so the scraper **enumerates the year pages from the
public XML sitemap** `https://www.pa.gov/en.sitemap.xml` (filtered to the PLRB
order paths). Each year page server-renders `<a href>` links to born-digital
decision PDFs under `/content/dam/.../plrb/{final|proposed}-orders/{year}/documents/{slug}.pdf`.

Full text is extracted with the shared `common.pdf_extract` extractor. Case
number, order type (Final vs Proposed), and the decision date are parsed from
the order body. No auth, no CAPTCHA.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — orders of the Pennsylvania Labor Relations Board are official works of Pennsylvania state government (edicts of a government agency) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially.
