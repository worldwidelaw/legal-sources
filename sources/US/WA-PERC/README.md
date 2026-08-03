# US/WA-PERC — Washington State Public Employment Relations Commission

Full text of every published decision of the **Washington State Public
Employment Relations Commission (PERC)**, the state's quasi-judicial agency
for public-sector labor relations. PERC adjudicates:

- **Unfair labor practice** complaints
- **Representation / unit-determination** cases (certifications, elections)
- **Interest arbitration**
- **Law-enforcement disciplinary arbitration**
- **Fact finding** and related matters

under Washington's public-employee collective-bargaining statutes
(RCW 41.56, 41.58, 41.59, 41.76, 41.80, the PSRA, etc.). Each decision
resolves a specific contested case = **case_law**.

Coverage: **~10,000 decisions, 1976–present**, across five collections
(decisions, advisory-opinions, interest-arbitrations,
law-enforcement-disciplinary-arbitration, marine-employees-commission).

## Access

The corpus is served by **Lexum's "Decisia" platform** at
`https://decisions.perc.wa.gov/`. No CAPTCHA, no auth, no JavaScript required.

1. **Enumerate by year, per collection.** The public UI is wrapped in the
   perc.wa.gov WordPress theme, but appending `?iframe=true` to any Decisia
   URL returns the raw content. Read the sidebar year links from
   `/waperc/{collection}/en/nav_date.do?iframe=true`, then page each year:
   ```
   /waperc/{collection}/en/{YYYY}/nav_date.do?page={N}&iframe=true
   ```
   (25 items per page) collecting item links of the form
   `/waperc/{collection}/en/item/{id}/index.do`.

2. **Fetch full text + metadata** from `{item}?iframe=true`:
   - A `<td class="label">`/`<td class="metadata">` table yields
     **Collection, Date, Case Number, Decision Maker, Case Type, Statute**.
   - The decision body lives in
     `<div id="document-content" class="lbh-document"> … <div class="documentcontent">`,
     which is cleaned to plain text (paragraph/line breaks preserved, tags
     stripped, entities decoded). Born-digital HTML — **no OCR needed**.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~10,000 decisions)
python bootstrap.py bootstrap-fast      # alias for the full pull (VPS wrapper)
```

## Output schema (case_law)

`_id`, `_source`, `_type`, `_fetched_at`, `item_id`, `collection`,
`case_number`, `decision_maker`, `case_type`, `statute`, `issuer`, `title`,
`text` (full decision text), `url`, `date` (ISO 8601), `jurisdiction`
(`US-WA`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Washington State Public Employment Relations Commission are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
