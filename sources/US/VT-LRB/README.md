# US/VT-LRB — Vermont Labor Relations Board Decisions

Full-text decisions of the **Vermont Labor Relations Board (VLRB)**, the
quasi-judicial state agency that adjudicates public- and private-sector
labor-relations disputes in Vermont. The Board decides grievances,
unfair-labor-practice charges, representation/election petitions, unit
determinations, and related contested cases under:

- **SELRA** — State Employees Labor Relations Act (3 V.S.A. ch. 27)
- **MELRA** — Municipal Employees Labor Relations Act (21 V.S.A. ch. 22)
- **Labor Relations for Teachers Act** (16 V.S.A. ch. 57)
- **JELRA** — Judiciary Employees Labor Relations Act
- the State Employees Grievance system

Each decision resolves a specific contested case → **case_law**.

## Source & method

The Board publishes its complete run of bound decision volumes as
per-volume **ZIP archives** linked from
<https://vlrb.vermont.gov/decisions/download> (Volume 1: 1977-78 through
the current volume). Volumes 1–34 are direct `Volume N.zip` links; later
volumes (35+) are linked via a Drupal `/node/` or `/document/` page whose
body carries the ZIP link. Each ZIP contains one born-digital PDF per
decision, named `Volume N/N-PPP <caption>.pdf`.

The scraper walks the download page, resolves every volume ZIP,
downloads each ZIP once, and extracts full text from each member PDF via
the shared `common.pdf_extract` extractor. The internal docket number
(`DOCKET NO. YY-NN`) and the decision date (`dated ... Nth day of Month,
YYYY`) are parsed from the decision body.

No authentication, no CAPTCHA — builds locally (requires a Python
interpreter with PyMuPDF for PDF extraction).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## Record schema

`_id`, `_source`, `_type` (case_law), `_fetched_at`, `record_id`,
`issuer`, `docket`, `volume`, `title`, `text` (full decision), `date`
(ISO 8601), `url`, `jurisdiction` (US-VT).

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Vermont Labor Relations Board are official edicts of a quasi-judicial Vermont state government body and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
