# US/MA-ATB — Massachusetts Appellate Tax Board

Full text of the decisions ("Findings of Fact and Reports") of the
**Massachusetts Appellate Tax Board (ATB)** — the quasi-judicial state agency
that adjudicates appeals from local property-tax abatement denials and from
state-tax determinations (corporate excise, income, sales/use, etc.). Each
decision resolves a specific taxpayer-vs-assessors / taxpayer-vs-Commissioner
controversy, so the corpus is **case_law**.

## Source

State Library of Massachusetts **DSpace 7 REST API** at
`https://archives.lib.state.ma.us/server/api` — open, no auth, no WAF. This is
the same repository used by `US/MA-SessionLaws`.

- **Enumeration:** Discover search scoped by author
  `dc.contributor.author:"Massachusetts. Appellate Tax Board."` (~1,114 decision
  items), paged at size 100 with `embed=bundles/bitstreams`.
- **Full text:** the DSpace-extracted plain-text bitstream in each item's `TEXT`
  bundle (sibling of the ORIGINAL PDF). Modern decisions are born-digital and
  extract cleanly — no local PDF/OCR step.
- **Docket number:** parsed from item metadata (`dc.description`) or the decision
  body (`Docket No. F339485`).

## Output schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title` (case caption),
`text` (full decision), `url` (DSpace handle), `date` (promulgated, ISO 8601),
`docket_number`, `court`, `jurisdiction` (`US-MA`).

## Usage

```bash
python3 bootstrap.py bootstrap --sample   # 12 sample decisions, newest first
python3 bootstrap.py bootstrap            # full pull (~1,114 decisions)
python3 bootstrap.py test-api             # connectivity / extraction test
```

## License

[Public Domain (US Government Work — Massachusetts)](https://www.law.cornell.edu/uscode/text/17/105) — Massachusetts Appellate Tax Board decisions are official state government works (judicial-type opinions / edicts of government) in the public domain. Digitized and served openly by the State Library of Massachusetts. Commercial use permitted.
