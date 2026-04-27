# DZ/SupremeCourt-Decisions — Algeria Supreme Court

## Source

- **Name**: المحكمة العليا (Supreme Court / Cour Suprême)
- **URL**: https://coursupreme.dz
- **Type**: case_law
- **Auth**: none
- **Coverage**: ~1,261 decisions (2000–2023)

## Method

WordPress REST API: `GET /wp-json/wp/v2/decision?per_page=100&page={n}`

Full text in `content.rendered` field. 13 pages total, ~1 minute to harvest.

## Fields

| Field | Description |
|-------|-------------|
| wp_id | WordPress post ID |
| title | Decision title |
| text | Full decision text (Arabic) |
| date | Publication date |
| decision_number | رقم القرار |
| decision_date | تاريخ القرار |
| subject | الموضوع |
| chamber_ids | Taxonomy term IDs for chamber classification |

## License

[Open Government Data](https://coursupreme.dz) — official decisions published by the Supreme Court of Algeria.
