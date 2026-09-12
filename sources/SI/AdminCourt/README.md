# SI/AdminCourt — Slovenian Administrative Court

**Source:** [https://sodnapraksa.si](https://sodnapraksa.si)
**Data types:** case_law

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Coverage

UPRS decisions of the Upravno sodišče Republike Slovenije (Administrative Court of
the Republic of Slovenia) — ~36,050 decisions from 2000 to the present, in Slovenian,
carrying ECLI identifiers of the form `ECLI:SI:UPRS:YYYY:*`.

## Access

`sodnapraksa.si` was rebuilt as a Vue SPA; the legacy `/search.php` endpoint now
302-redirects to `/iskanje/{base64-search-state}` and serves no HTML results.

The SPA is backed by an unauthenticated JSON search API that returns the full text
of every hit inline, so one paged request yields 100 complete decisions with no
per-document fetch:

```
POST https://sodnapraksa.si/backend/api/search/documents
{
  "simpleSearch": false,
  "query": {"q": "*", "f": [{"n": "docType", "v": ["uprs"], "and": false}]},
  "page": 0, "pageSize": 100,
  "sortField": "date", "sortDirection": "ASC"
}
```

`pageSize` above 100 is rejected. Documents are addressable in the UI at
`https://sodnapraksa.si/dokument/uprs/{id}`.

Text is assembled from three HTML fields on each hit:

| API field    | Slovenian section | Content            |
|--------------|-------------------|--------------------|
| `coreText`   | Jedro             | Summary / holding  |
| `ruling`     | Izrek             | Operative ruling   |
| `motivation` | Obrazložitev      | Reasoning          |

## Crawl strategy

Results are ordered by decision date **ascending**, so newly published decisions
always land at the tail and earlier pages never shift. That makes the page-number
checkpoint in `data/uprs_checkpoint.json` safe to resume from — a relaunched run
skips completed pages with no network calls and advances monotonically.

If the API reports zero hits, the scraper raises rather than reporting a successful
empty run, so an upstream change fails loud instead of silently ingesting nothing.

## Usage

```bash
python bootstrap.py test                 # connectivity + extraction check
python bootstrap.py bootstrap --sample   # 10+ validation samples
python bootstrap.py bootstrap            # full corpus → data/records.jsonl
python bootstrap.py bootstrap-fast       # alias for the full pull (fleet runner)
python bootstrap.py update               # incremental, newest-first walk
```
