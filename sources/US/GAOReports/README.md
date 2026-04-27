# US/GAOReports — GAO Reports & Comptroller General Decisions

**Source:** GovInfo (U.S. Government Publishing Office)
**URL:** https://www.govinfo.gov/app/collection/gaoreports
**Type:** Doctrine
**Auth:** None required
**Records:** ~16,569 reports (1989–2008)

## Data Access

Uses the GovInfo public wssearch API for sample/search and XML sitemaps
for full enumeration (the wssearch API caps at 2,000 results).

Full text is extracted from the HTML content pages at
`govinfo.gov/content/pkg/{id}/html/{id}.htm`. PDF fallback is available
via `common.pdf_extract`.

## Document Types

- GAO Reports (GAO-YY-NNNN) — audits, evaluations, special studies
- Testimonies (GAO-YY-NNNNT) — congressional testimony
- Correspondence (GAO-YY-NNNNR) — responses to congressional requests
- Comptroller General Decisions (B-NNNNNN) — bid protests, appropriation rulings

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap --full     # All ~16,569 reports
python bootstrap.py updates --since 2024-01-01
```

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105.
