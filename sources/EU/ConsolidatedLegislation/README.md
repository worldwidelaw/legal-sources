# EU/ConsolidatedLegislation — Consolidated (In-force) EU Law

Official **consolidated** versions of EU legal acts — **sector 0** of the
EUR-Lex taxonomy. A consolidated text is the base act with every subsequent
amendment editorially woven back in, so the reader sees the law *as it currently
stands* (or as it will stand on a future effective date). This is what
practitioners actually read; the as-adopted base act (EU/EUR-Lex, sector 3)
shows the law only in its original, un-amended form.

## What this covers

- **~11,200 documents** — the single most recent consolidation per base act.
- Consolidated Regulations, Directives, and Decisions across all policy areas,
  from the earliest EU law to the present (and future-effective consolidations).
- Full consolidated text (the integrated body, articles and annexes), not just
  metadata.

A sector-0 CELEX has the form `0{YYYY}{TYPE}{NNNN}-{YYYYMMDD}` — the base act's
year/type/number followed by the consolidated version's date. Example:
`02000L0060-20260510` = the Water Framework Directive consolidated to 10 May
2026. Each base act is re-consolidated whenever it is amended (the WFD alone has
nine consolidations); this source keeps only the latest per base act to give one
clean current text and avoid a dozen near-identical historical snapshots.

Directly closes issue **#1187** (consolidated Water Framework Directive
amendments missing from EUR-Lex coverage).

## How it works

1. **Enumerate** every sector-0 act via the public CELLAR SPARQL endpoint,
   scoped per original year (`^0{YYYY}`) — the full ~32,800-consolidation corpus
   exceeds the endpoint's ~10,000-row OFFSET ceiling, while the busiest single
   year is ~1,400 rows. Within each year the consolidations are reduced to the
   latest one per base act.
2. **Fetch** the full consolidated text via CELLAR HTTP content negotiation
   (`GET /resource/celex/{CELEX}`, `Accept: application/xhtml+xml`,
   `Accept-Language: en`). This serves the consolidated Formex/xHTML body and
   bypasses the eur-lex.europa.eu AWS-WAF that 202-challenges datacenter IPs, so
   it is fleet-safe.
3. **Normalize** to the standard schema (legislation).

Same CELLAR recipe as `EU/Treaties` (sector 1) and `EU/ComplementaryLegislation`
(sector 4), narrowed to sector 0 with per-year scoping and latest-per-base
reduction.

## Usage

```bash
# 15-record sample
python3 sources/EU/ConsolidatedLegislation/bootstrap.py bootstrap --sample

# full corpus (streams to data/records.jsonl)
python3 sources/EU/ConsolidatedLegislation/bootstrap.py bootstrap-fast

# quick endpoint check
python3 sources/EU/ConsolidatedLegislation/bootstrap.py test
```

## Record schema

| field    | description                                            |
|----------|--------------------------------------------------------|
| `_id`    | consolidated CELEX (e.g. `02000L0060-20260510`)        |
| `_source`| `EU/ConsolidatedLegislation`                           |
| `_type`  | `legislation`                                          |
| `title`  | English expression title of the act                    |
| `text`   | full consolidated text (integrated body)               |
| `date`   | date of the consolidated version (ISO 8601)            |
| `url`    | EUR-Lex CELEX permalink                                |

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) —
consolidated legal texts published by the Publications Office are reusable,
including for commercial purposes, with attribution requested. The consolidated
versions are editorial documentation tools; the authentic text remains the
Official Journal version.
