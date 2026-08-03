# US/NJ-EthicsDecisions — New Jersey State Ethics Commission, Final Decisions

Full text of the final agency actions of the **New Jersey State Ethics
Commission** (SEC, Dept. of Law & Public Safety) under the Conflicts of
Interest Law (N.J.S.A. 52:13D-12 et seq.) and Executive Order 14: final
orders, consent orders, penalty-waiver determinations, casino/cannabis
post-employment waivers, Section 19 and EO-14 exceptions, plus a handful of
advisory opinions, adopted codes of ethics and a rule adoption.

Case-specific final agency actions on a named respondent/requester are
`case_law`; advisory opinions, codes of ethics and rule adoptions are
`doctrine` (classified per record).

> **Distinct from** `US/NJ-SchoolEthics` (School Ethics Commission advisory
> opinions), `US/NJ-PERC`, `US/NJ-OAL` and `US/NJ-AGOpinions`.

## Source / access

Enumerated via the official New Jersey open-data (Socrata) dataset
**`54br-q95u`** — "State Ethics Commission Final Decisions":

```
https://data.nj.gov/resource/54br-q95u.json?$limit=1000
```

A single call returns all ~695 rows (no auth, key or JavaScript). Each row
carries `file_type.url` = a direct born-digital PDF on
`https://nj.gov/ethics/docs/final/`, plus `type`, `firstname`/`lastname`
(party), `agency_department`, `case_number`, `final_agency_action`, `year`,
`month`. PDFs are extracted with the shared `common.pdf_extract` backend
(OCR fallback for any scan). A couple of rows link `.zip` archives and are
skipped, leaving ~689 PDF decisions.

The decision date is parsed from the `Month DD, YYYY` line near the top of
the PDF, falling back to the dataset `year`/`month` (portal posting date).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (~689 decisions)
```

## License

[Public Domain (State of New Jersey Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — final decisions of the New Jersey State Ethics Commission are official public records of the State of New Jersey, released as open data with no copyright restriction. Commercial use permitted; no attribution required.
