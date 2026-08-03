# US/NJ-PERC — New Jersey Public Employment Relations Commission (Decisions)

Full-text **case law** of the **New Jersey Public Employment Relations
Commission (PERC)**, the state's quasi-judicial agency that administers the
New Jersey Employer-Employee Relations Act (N.J.S.A. 34:13A). PERC adjudicates
public-sector labor-relations disputes — scope-of-negotiations determinations,
restraints of binding arbitration, unfair-practice charges, representation /
unit-clarification cases, and related contested matters. Each Commission
decision resolves a specific contested case = `case_law`.

- **~5,000+ decisions**, 1980–present (~100–160/year)
- **Full text** extracted from each decision PDF
- **No CAPTCHA, no auth**

## Data source

The official decision database is a **Lotus Domino** application at
`https://www.perc.state.nj.us/percdecisions.nsf`. Decisions are stored as one
PDF attachment each, exposed through the `IssuedDecisions` view, which is
**categorized by calendar year**. The full corpus is enumerated by restricting
the view to each year:

```
GET /percdecisions.nsf/IssuedDecisions?OpenView&RestrictToCategory={YEAR}&Count=2000
```

Each row links to the decision PDF:

```
/percdecisions.nsf/IssuedDecisions/{UNID}/$File/{citation}.pdf?OpenElement
```

where `{UNID}` is a stable Domino document universal id (32-hex) used as the
record key, and the attachment filename encodes the citation (e.g.
`PERC 90-125.pdf` → *P.E.R.C. NO. 90-125*; `PERC 2026 46.pdf` →
*P.E.R.C. NO. 2026-46*). The issued date is parsed from the
`ISSUED: <Month DD, YYYY>` stamp on the decision.

> **Note:** the PDF host requires a browser `User-Agent` — plain requests
> receive HTTP 403. The scraper downloads the bytes with a browser UA and
> hands them to the shared `common.pdf_extract` extractor.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~5,000+ decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Record schema

| field | description |
|-------|-------------|
| `_id` | `US/NJ-PERC/{UNID}` |
| `_source` | `US/NJ-PERC` |
| `_type` | `case_law` |
| `citation` | P.E.R.C. citation (e.g. `P.E.R.C. NO. 2026-46`) |
| `title` | decision title (the citation) |
| `text` | full decision text (PDF extract) |
| `date` | issued date (ISO 8601) |
| `url` | link to the decision PDF |
| `jurisdiction` | `US-NJ` |

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the New Jersey Public Employment Relations Commission are official works of a quasi-judicial New Jersey state government body and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
