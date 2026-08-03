# US/LA-EAB — Louisiana Ethics Adjudicatory Board Decisions

Full-text adjudicatory decisions of the **Louisiana Ethics Adjudicatory Board
(EAB)**, a panel of administrative law judges of the **Louisiana Division of
Administrative Law (DAL)**.

## What this is

The EAB hears and decides the ethics charges instituted by the **Louisiana
Board of Ethics** under the Code of Governmental Ethics (La. R.S. 42:1101 et
seq.) and the Campaign Finance Disclosure Act. Each **Decision and Order**
resolves a specific contested case against a named respondent (a public
official, public employee or candidate), making findings of fact and
conclusions of law and imposing any penalty. These are adjudicatory rulings of
a Louisiana state tribunal = **case_law** (government edicts).

Sibling of the project's other central-panel administrative adjudicators:
`US/MA-DALA`, `US/FL-DOAH`, `US/NC-OAH`, `US/CO-OAC`, `US/NJ-OAL`,
`US/CA-OAH-SpecialEd`.

## How it works

The DAL publishes each decision as a born-digital PDF on
<https://www.adminlaw.la.gov/>. The **Ethics Decisions** page is a Solr-backed
search result list, paginated with `?_page=N` (10 results per page). Each
result links directly to the decision PDF on the DAL's S3 bucket
(`redball-la-solr.s3.us-east-2.amazonaws.com/<key>.pdf`), and the result title
carries the docket number and respondent name, e.g.
`2025-20439-BOE-A - Brian J. Henly Decision.pdf`.

- **Enumerate:** `GET /ethics-decisions/?_page=N`, paging until no new results.
- **Per decision:** download the S3 PDF and extract the embedded text layer via
  the shared `common.pdf_extract` helper. The docket number and respondent are
  parsed from the result title; the decision date from the body
  (`"Rendered and signed on <Month DD, YYYY> ..."`).

No CAPTCHA, no authentication. Reachable locally.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # Full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Louisiana Ethics Adjudicatory Board are official works of Louisiana state government (edicts of government) and are not subject to copyright. Free to use, including commercially.
