# US/DE-PERB — Delaware Public Employment Relations Board Decisions

Full text of the decisions of the **Delaware Public Employment Relations
Board (PERB)**, the quasi-judicial state agency that adjudicates public-
sector labor-relations disputes in Delaware under the Public Employment
Relations Act (19 Del. C. ch. 13), the Police Officers' and Firefighters'
Employment Relations Act (19 Del. C. ch. 16), and the Public School
Employment Relations Act (14 Del. C. ch. 40).

The Board decides unfair-labor-practice charges, representation and
unit-clarification petitions, election objections, binding-interest-
arbitration disputes, and related contested cases. Each decision resolves
a specific contested case = **case_law**.

## Source

- **Index:** https://perb.delaware.gov/decisions/
- **Coverage:** ~1,000+ decisions, 1984 – present
- **Format:** born-digital PDFs (one per decision) on a WordPress site

## Build recipe

No auth, no CAPTCHA — builds locally.

1. Fetch the `/decisions/` index → resolve every year-listing page
   (`/decisions/{YYYY}-decisions/`, a few recent years at `/{YYYY}-decisions/`).
2. For each year page, extract every `.pdf` link on the `wp-content/uploads` store.
3. Download each PDF once and extract full text with the shared
   `common.pdf_extract` extractor.
4. Parse the case/charge number (`No. YY-MM-NNNN`) and decision date
   (`DATE: Month D, YYYY`) from the decision body; the filename supplies
   the caption/title and a fallback year.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) —
Decisions of the Delaware Public Employment Relations Board are official
works of Delaware state government (edicts of a quasi-judicial government
body) and are not subject to copyright under the government-edicts
doctrine. Free to use, including commercially.
