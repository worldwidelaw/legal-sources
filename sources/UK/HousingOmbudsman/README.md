# UK/HousingOmbudsman — Housing Ombudsman Service (England) Determinations

Published determinations of the **Housing Ombudsman Service**, the statutory
dispute-resolution body established under the Housing Act 1996 and operating the
Housing Ombudsman Scheme approved by the Secretary of State. The Ombudsman
investigates complaints by residents against member landlords (social
landlords, local authorities and voluntary members) and decides whether the
landlord's handling amounted to **maladministration** or **service failure**,
imposing binding orders and recommendations.

- **Coverage:** ~17,000+ determinations (England; 2020–present)
- **Type:** case_law
- **Full text:** embedded in each decision page HTML — a metadata table
  (Case ID, Decision type, Jurisdiction, Landlord, Landlord type, Occupancy,
  Date) followed by the Background / What the complaint is about / Our decision
  (determination) / Reasons / Orders narrative. Born-digital, no OCR.

## How it works

1. **Discovery** — the decisions archive at `/decisions/` is a paginated
   WordPress listing (`/decisions/page/{n}/`, ~1,674 pages). Each page links to
   individual determination permalinks `/decisions/{landlord-slug}-{caseref}/`.
2. **Extraction** — there is **no** public WP REST API for the `decision` post
   type (`/wp-json/wp/v2/decision` → 404), so each determination page is fetched
   and its embedded HTML content parsed directly.
3. **Normalization** — `case_id` is the primary key; `date` is parsed from the
   metadata table's Date cell.

```bash
python bootstrap.py bootstrap --sample   # 12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

## License

> ⚠️ **Commercial use restricted.** No explicit open licence is published.

[Custom terms — © Housing Ombudsman Service](https://www.housing-ombudsman.org.uk/legal/)
— the site footer asserts "© Housing Ombudsman Service" and no Open Government
Licence statement is present. Determinations are published for public
information; attribution required and commercial use is flagged pending
confirmation of licensing terms.

## Related sources

- **UK/ScotHousingChamber** — First-tier Tribunal for Scotland (Housing &
  Property Chamber); the Scottish counterpart.
- **UK/PHSO** — Parliamentary and Health Service Ombudsman.
- **UK/FinancialOmbudsman** — Financial Ombudsman Service decisions.
