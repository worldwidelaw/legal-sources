# TZ/FCC-Decisions — Tanzania Fair Competition Commission

Competition decisions (merger determinations, complaint rulings), provisional
findings, and official speeches published by the Fair Competition Commission of
Tanzania.

Data is fetched from the FCC's public REST API at
`https://www.fcc.go.tz/content/v1/publications/published-publications/list`.

The API is paginated (10 items/page) and returns JSON with bilingual
(English/Swahili) titles and descriptions. The whole published corpus is 17
publications (`pagination.totalItems`).

Full text has two shapes:

* **Merger determinations** carry no attachment — the published decision
  narrative (parties, market definition, s.5(6) analysis, outcome) *is* the
  `description` field, typically 500–850 characters.
* **Provisional findings and speeches** carry a PDF `attachment`, extracted via
  `common/pdf_extract`. The Coca-Cola provisional findings (FCC/COMP.9/2021)
  yields ~81K characters; speeches 12K–23K.

## Attachment host rewrite

The CMS stores some attachment URLs against its own internal address
(`http://10.1.90.177/attachments/...`), which is unroutable from outside the FCC
network. The same paths are served from `https://www.fcc.go.tz`, so the scraper
rewrites the authority and percent-encodes the (space-bearing) filenames. Five of
the six speeches are only reachable this way.

## Usage

```bash
python bootstrap.py test-api             # connectivity probe
python bootstrap.py bootstrap --sample   # sample records into sample/
python bootstrap.py bootstrap-fast       # full corpus → data/records.jsonl
python bootstrap.py updates --since ISO  # re-walk, filtered on lastModified
```

## Record types

| Category | `_type` | Count |
|----------|---------|-------|
| Decisions | `case_law` | 10 |
| Provisional Findings | `case_law` | 1 |
| Public Speeches | `doctrine` | 6 |

## License

[Open Government Data (Tanzania)](https://www.fcc.go.tz/) — official
government publications, no stated restrictions.
