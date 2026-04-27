# UA/EDRSR — Unified State Register of Court Decisions (ЄДРСР)

**Source**: State Judicial Administration of Ukraine
**URL**: https://reyestr.court.gov.ua
**Data types**: Case law
**Auth**: None (Open Data)
**License**: Creative Commons Attribution

## Overview

Ukraine's official court decision register — the largest in Europe with 132M+
documents spanning 2006 to present. All courts, all jurisdictions, all case types.

## Data Access

**Metadata**: Yearly ZIP archives on data.gov.ua containing tab-delimited CSV files.
Each ZIP contains `documents.csv` (metadata for all decisions in that year) plus
reference tables (`courts.csv`, `justice_kinds.csv`, etc.).

**Full Text**: HTML files at `http://od.reyestr.court.gov.ua/files/{hash}.html`
(open data subdomain, no CAPTCHA, no auth). Encoding is windows-1251.

Note: The main site (`reyestr.court.gov.ua`) requires CAPTCHA. The open data
subdomain (`od.reyestr.court.gov.ua`) does not.

## Record Schema

| Field | Description |
|-------|-------------|
| doc_id | Unique document ID |
| title | Case number + court name |
| text | Full text of the decision (Ukrainian) |
| date | Adjudication date |
| url | Link to full text HTML |
| court_code | Court identifier |
| court_name | Court name (Ukrainian) |
| judge | Judge name(s) |
| justice_kind | Type of justice |
| judgment_form | Form of judgment |

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — free reuse with attribution.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records (2006 data)
python bootstrap.py bootstrap            # Full pull (WARNING: 132M+ docs)
python bootstrap.py test-api             # Connectivity test
```
