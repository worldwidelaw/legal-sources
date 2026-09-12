# DK/SKAT - Danish Tax Authority Rulings

Fetches Danish tax rulings and binding answers (bindende svar) from Skatterådet and
Skattestyrelsen, published on Retsinformation.

## Data Source

- **Source**: Retsinformation (Danish Legal Information System)
- **URL**: https://www.retsinformation.dk
- **Ministry**: Skatteministeriet (Ministry of Taxation)
- **Authority**: Skatterådet (Tax Council)

## Document Types

- **AFG** (Afgørelser): Tax rulings and decisions
  - Binding rulings (bindende svar)
  - Tax decisions from Skatterådet

## API

Enumeration uses Retsinformation's own search API, filtered to the AFG
(Afgørelse) document type:

```
https://www.retsinformation.dk/api/documentsearch?ps=100&dt=230&page={n}
```

`ps` is capped server-side at 100, so the ~6,000 AFG documents take ~61 pages.
Each hit carries a `retsinfoLink` (an ELI path); the full text comes from its
LexDania XML:

```
https://www.retsinformation.dk{retsinfoLink}/dan/xml
```

AFG covers several ministries, so hits are kept only when `ressortName`
contains "Skatte" (Skatteministeriet / Skatte- og Vækstministeriet).

No authentication required - open data.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Full bootstrap, concurrent downloads + batched JSONL writes (fleet path)
python bootstrap.py bootstrap-fast

# Incremental update
python bootstrap.py update
```

## Notes

- Coverage runs 1954-present (the old ELI number scan only reached back to 2020)
- Full text is extracted from the LexDania XML structure (Titel / Resume /
  TekstGruppe), HTML-unescaped and whitespace-normalized
- Requests carry both a `(connect, read)` socket timeout and a wall-clock
  deadline, so a trickling response cannot wedge the run (issue #1389)

## History

Before 2026-08 this scraper blind-scanned `/eli/retsinfo/{year}/{number}` over
numbers 9000-11500 for years 2020-present — ~17,500 requests, most of them
404s. Issue #1389 reported the fleet run hanging ~29 minutes on "Scanning year
2026" with an empty `records.jsonl`. Replaced by the search API above.

## License

[Open Data](https://www.retsinformation.dk) — Danish tax rulings are freely available via Retsinformation.
