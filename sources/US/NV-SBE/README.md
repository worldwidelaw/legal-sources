# US/NV-SBE — Nevada State Board of Equalization: Notices of Decision

Full text of the adjudicative **Notices of Decision** issued by the **Nevada
State Board of Equalization (SBE)** — Nevada's central property-tax appeal
board. Under NRS ch. 361 the SBE hears petitions for review of property
valuations (county assessor v. taxpayer, and direct taxpayer appeals) and
issues a written decision resolving each contested case. These adjudications
of specific cases are classified as **case_law**.

## Data access

Source: **tax.nv.gov** (Nevada Department of Taxation, WordPress).

The legacy `/Boards/State_Board_of_Equalization_Forms/SBE_Decision_Letters/`
directory tree was retired in the site's WordPress migration (those paths now
302 to the homepage). Decisions are now published as born-digital PDF
attachments under `/wp-content/uploads/YYYY/MM/` and enumerated via the
WordPress REST media API:

```
GET /wp-json/wp/v2/media?media_type=application&per_page=100&page=N
```

Each SBE Notice of Decision attachment has a **slug that is its case number**
in the form `YY-NNN` (e.g. `23-117` = *Clark County Assessor v. Aria Resort
and Casino*). The scraper sweeps all media pages, keeps attachments whose slug
matches the case-number pattern, downloads each PDF and extracts its
born-digital text layer via `common.pdf_extract`.

> **UA note:** the `/wp-content` PDFs and the `wp-json` REST API return HTTP
> 200 only for a browser User-Agent; a bare requests/urllib UA gets 403. The
> scraper fetches every URL via `curl` with a Chrome UA and passes the raw
> bytes to the extractor.

No JavaScript, no CAPTCHA, no auth.

## Record schema

Each normalized record contains `_id` (`US/NV-SBE/{case_no}`), `_source`,
`_type` (`case_law`), `_fetched_at`, `case_no`, `court` (Nevada State Board of
Equalization), `title` (case caption/parties), `text` (full decision text),
`url`, `date`, and `jurisdiction` (`US-NV`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Notices of Decision of the Nevada State Board of Equalization are official state-government works in the public domain under the government-edicts doctrine. No attribution required; commercial use permitted.
