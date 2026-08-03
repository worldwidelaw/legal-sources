# US/FL-JudicialEthics — Florida Judicial Ethics Advisory Committee (JEAC)

Advisory opinions issued by the Florida Supreme Court's **Judicial Ethics
Advisory Committee (JEAC)**. The JEAC renders written opinions interpreting the
application of the **Florida Code of Judicial Conduct** to specific
circumstances confronting or affecting a judge or judicial candidate. Each
opinion states the issue(s), the Committee's answer, the facts, the applicable
Canons, and its discussion = **doctrine** (the Committee's official written
interpretation of the judicial-conduct rules).

- **Coverage:** ~1,391 opinions, 1972 to present.
- **Full text:** born-digital HTML retrieved from the site's public Ibexa
  (eZ Platform) JSON content API — no OCR, no PDF, no CAPTCHA, no auth.
- **Distinct from `US/FL-EthicsOpinions`** (Florida Commission on Ethics,
  which covers executive-branch public officers). This source is the
  **judicial** branch — the Supreme Court's advisory committee for judges.

## Access

`jeac.flcourts.gov` is a Next.js front-end backed by an Ibexa DXP whose public
content API is served from the media host:

```
https://flcourts-media.ccplatform.net/api/data/fetch
```

1. **List per-year containers** (folders under the "Opinions by Year" container,
   Ibexa location `846113`):
   ```
   ?loadContent=false&limit=200&parentLocationID=846113&classFilter=folder
   ```
2. **List a year's opinions** with full content fields:
   ```
   ?loadContent=true&limit=200&parentLocationID={year_location}
       &sortClause[0]=opinion&sortClause[1]=disposition_date&sortClause[2]=DESC
   ```

Each `jeac_opinion` item carries structured fields: `opinion` (the `YYYY-NN`
number), `canons`, `date_of_issue`, `subject`, `issue`, `facts`, `discussion`,
`references`, and `migrated_html`. The full opinion body is born-digital HTML in
`migrated_html`; when empty, the structured Issue / Facts / Discussion /
References / Subject sections are assembled instead.

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # Full pull (all opinions)
python bootstrap.py bootstrap-fast      # Alias for full pull (VPS wrapper)
```

## License

[Public Domain (Florida State Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — advisory opinions of the Florida Judicial Ethics Advisory Committee are official public records of the State of Florida, published for public use with no copyright restriction. Commercial use permitted; no attribution required.
