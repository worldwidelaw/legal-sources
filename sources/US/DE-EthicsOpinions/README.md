# US/DE-EthicsOpinions — Delaware Public Integrity Commission, Advisory Opinion Synopses

Full text of the **Delaware Public Integrity Commission** (PIC) advisory-opinion
synopses and case decisions under the State Employees', Officers' and Officials'
Code of Conduct (29 Del. C. ch. 58), the Financial Disclosure law, the Lobbying
law, and the dual-compensation policy.

The PIC publishes its opinion corpus not as one file per opinion but as
**topic-consolidated, born-digital PDFs** — each compiling every opinion on
that topic (1991–2023) with the applicable statute and the facts/holding of
each matter. These are the Commission's authoritative published interpretations
= `doctrine`.

> **Distinct from** `US/DE-AGOpinions`, `US/DE-PERB`, `US/DE-Courts`.

## Source / access

Six server-rendered WordPress listing pages on `depic.delaware.gov` link the
topic PDFs under `/wp-content/uploads/`:

- `/code-of-conduct/opinion-synopsis/`
- `/code-of-conduct/pic-casedecisions/`
- `/financial-disclosure/opinion-synopses/`
- `/lobbying/lobbying-opinion-synopses/`
- `/compensation-policy/case-decisions/`
- `/compensation-policy/synopses-opinions/`

The scraper scans all six, collects every `wp-content` PDF, dedups by URL
(~21 unique), and extracts full text with the shared `common.pdf_extract`
backend (born-digital; OCR fallback for any scan). Topic files are large (tens
of KB to over 1.4M characters). The date is the latest year in each topic's
coverage range.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (~21 topic PDFs)
```

## License

[Public Domain (State of Delaware Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — advisory opinion synopses and case decisions of the Delaware Public Integrity Commission are official public records of the State of Delaware, released for public use with no copyright restriction. Commercial use permitted; no attribution required.
