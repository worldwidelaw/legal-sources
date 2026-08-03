# US/CA-PERB-FactFinding — California PERB Fact-Finding Reports

Fact-finding reports of the **California Public Employment Relations Board
(PERB)**. Under California's public-sector labor-relations statutes (the MMBA,
EERA, HEERA, the Dills Act, etc.), when a public employer and a recognized
employee organization reach impasse in bargaining, a tripartite fact-finding
panel chaired by a neutral factfinder holds hearings, makes written **findings
of fact** on the disputed issues, and issues a **report with recommended terms
of settlement** for that specific dispute. Each report resolves a specific
contested impasse between named parties = **case_law**.

Sibling of **US/CA-PERB** (PERB Board Decisions), which covers the
unfair-practice / representation adjudication track of the same agency.

## Access

`perb.ca.gov` is a WordPress site. Fact-finding reports are a custom post type
`fact-finder-report`, enumerable via the public WP REST API (no CAPTCHA, no
auth):

```
GET /wp-json/wp/v2/fact-finder-report?per_page=100&page={N}    # ~914 posts
```

Each post's single child media attachment (the report PDF) is obtained via:

```
GET /wp-json/wp/v2/media?parent={postId}
    -> source_url = /wp-content/uploads/YYYY/MM/FR{NNNN}.pdf
```

The PDFs are born-digital text-layer files; text is extracted with
`common.pdf_extract` (no OCR). The post title carries the employer + union
party caption; `FR{NNNN}` is the fact-finding report number.

## Usage

```bash
python bootstrap.py test-api              # connectivity + extraction check
python bootstrap.py bootstrap --sample    # ~12 sample records
python bootstrap.py bootstrap             # full pull (~914 reports)
```

## Data

- `_type`: `case_law`
- ~914 fact-finding reports
- Fields: `report_number`, `parties`, `title`, `text` (full body), `date`,
  `url`, `pdf_url`, `jurisdiction` (US-CA)

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
official works of the California Public Employment Relations Board are
government edicts in the public domain. Commercial use permitted; no
attribution required.
