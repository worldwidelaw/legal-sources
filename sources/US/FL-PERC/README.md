# US/FL-PERC — Florida Public Employees Relations Commission (Final Orders)

Full text of the **Final Orders** of the **Florida Public Employees
Relations Commission (PERC)** — Florida's independent quasi-judicial
agency for public-sector labor relations under Part II of Chapter 447,
Florida Statutes. PERC adjudicates:

- unfair labor practice charges,
- bargaining-unit determinations,
- representation / decertification elections and employee-organization
  registrations,
- career-service and veterans'-preference public-employment appeals.

Each order resolves a specific contested case → `case_law`.

## Source & access

Per section 120.53, Florida Statutes, every agency Final Order issued
after 1 July 2015 is published on the Division of Administrative
Hearings' **"Florida Agency Indexed Orders" (FLAIO)** database at
<https://www.doah.state.fl.us/FLAIO/> (PERC = agency code `PER`).

The database is a stateful classic-ASP session app:

1. `GET /FLAIO/` — seed the session cookie.
2. `POST /FLAIO/searchAction.asp?sT=byOther` with `Agency=PER`,
   `DocType=ALL`, `Subject=ALL` and a wide `IssueDate` range — stores
   the result set in the session and redirects to `flaioDisplay.asp`.
3. `GET /FLAIO/flaioDisplay.asp?pc={N}` — page `N` of the results
   (50 rows/page, ~142 pages, ~7,000 orders).

Each result row carries the agency case number, DOAH order number,
issue date, document type and subject, and links a **born-digital
text-layer PDF** on the DOAH file host
(`doah.state.fl.us/FLAID/PER/{YEAR}/PER_...pdf`). Text is extracted via
`common.pdf_extract`. No JavaScript, no CAPTCHA, no auth.

### Not a duplicate of US/FL-DOAH

`US/FL-DOAH` (`/ROS/`) holds the **ALJ Recommended Orders** issued by
the Division of Administrative Hearings in Chapter 120 contested cases.
`US/FL-PERC` holds **PERC's own agency Final Orders** in Chapter 447
labor cases. The two corpora do not overlap.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one-PDF check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~7,000 orders)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105)
— Orders of the Florida Public Employees Relations Commission are
official Florida state-government works in the public domain under the
government-edicts doctrine. No attribution required; commercial use
permitted.
