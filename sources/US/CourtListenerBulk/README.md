# US/CourtListenerBulk — CourtListener Bulk Data (Free Law Project)

Quarterly PostgreSQL CSV dumps from CourtListener, served from the public
AWS S3 bucket `com-courtlistener-storage`. ~10M federal and state court
opinions across 2,000+ courts, no API token required.

This source supersedes `US/CaselawAccessProject` once a full bootstrap
completes. The CL dataset is a cleaned superset of Harvard CAP (Free Law
Project manually corrected 1M+ items) plus everything from 2024–present.

## What it does

`bootstrap.py` performs a streaming, in-memory join across five CSVs:

```
opinions ──┐
           ├─→ joined record per opinion
clusters ──┤
           ├─→ normalized to LDH document
dockets ──┤
           │
courts  ──┘
```

For each opinion row in `opinions-YYYY-MM-DD.csv.bz2`, we look up its
`cluster_id` in the cluster table (which carries `case_name`, `date_filed`,
`precedential_status`, and `docket_id`), then the docket (`docket_number`,
`court_id`), then the court (`full_name`, `jurisdiction`).

Opinions where the cluster or docket has `blocked == 't'` are skipped —
those are individuals who requested removal under CL's privacy policy
([source](https://www.courtlistener.com/help/removing-content/)).

## Output schema

Each emitted document looks like:

```json
{
  "_id": "cl-opinion-7459048",
  "_source": "US/CourtListenerBulk",
  "_type": "case_law",
  "_fetched_at": "2026-04-24T...",
  "title": "Free v. United States",
  "text": "<opinion>...</opinion>",
  "date": "2011-02-22",
  "url": "https://www.courtlistener.com/opinion/9235844/free-v-united-states/",
  "court": "Supreme Court of North Carolina",
  "court_id": "nc",
  "court_jurisdiction": "S",
  "case_number": "2:09-cv-02161",
  "case_name_full": "Free v. United States",
  "author": "Bailes",
  "per_curiam": false,
  "opinion_type": "020lead",
  "precedential_status": "Published",
  "opinion_id": "7459048",
  "cluster_id": "9235844",
  "docket_id": "66502751",
  "page_count": null,
  "extracted_by_ocr": false,
  "license": "Public domain (US government works); no known copyright restrictions",
  "original_source": "CourtListener / Free Law Project"
}
```

The `text` field falls back through these columns in order until one is
non-empty: `html_with_citations` → `html_columbia` → `html_lawbox` →
`html_anon_2020` → `xml_harvard` → `html` → `plain_text`. A small
fraction of opinions are PDF-only with no extracted text and emit with
`text == ""`.

## Running

### Sample mode (proves the pipeline)

```
python runner.py sample US/CourtListenerBulk
```

The sample runs a cluster-first join:

1. Lists the latest dump date shared by all four required tables in S3
   (e.g. `2026-03-31`).
2. Streams `opinion-clusters-{date}.csv.bz2` and loads up to 200 K
   clusters into an in-memory pool.
3. Streams `dockets-{date}.csv.bz2` and retains dockets whose `id`
   matches one of the pool's `docket_id`s, until ~2 K cluster→docket
   pairs are joined.
4. Streams `opinions-{date}.csv.bz2` and emits the first 10 opinions
   whose `cluster_id` is in the joined pool.
5. Loads the small `courts-{date}.csv.bz2` fully.
6. Writes 10 normalized documents to `sample/`.

**Bandwidth requirement.** Each scan is bounded by
`sample.streaming_max_bytes` (default 500 MB compressed), but the
opinion-clusters file alone is ~2.5 GB compressed and dockets ~5 GB.
On a laptop link the first run may pull ~150–300 MB compressed before
emitting a record. Set `CLBULK_CACHE_DIR=/path` to tee each downloaded
file to local disk; subsequent runs read from the cache and complete
in seconds.

```
CLBULK_CACHE_DIR=$HOME/.cache/clbulk python runner.py sample US/CourtListenerBulk
```

If you can't afford the bandwidth locally, run the sample on the
Hetzner ingestor — it has fast egress and produces the same `sample/`
output, which can then be committed back to the repo.

### Full bootstrap (production — Hetzner)

```
python bootstrap.py bootstrap                   # default: chunked gzip JSONL
python bootstrap.py bootstrap --chunk-size 500000  # custom chunk size
```

Default mode writes gzip-compressed chunked JSONL files to `data/`:
`chunk_0000.jsonl.gz`, `chunk_0001.jsonl.gz`, etc. Each chunk holds up to
500K records. At ~11 KB/record and ~6× gzip ratio, each chunk is ~900 MB
and the full ~10M opinions produce ~15-20 GB total — fits on a 38 GB CX23.

The join still requires courts/dockets/clusters in SQLite before opinion
iteration begins. Memory budget: ~4–6 GB peak.

The old single-file mode (`bootstrap-legacy`) wrote a monolithic
`data/records.jsonl` that reached ~110 GB for 10M records, which filled
the VPS disk at 2.2M records (see issue #559).

### Incremental updates

`fetch_updates(since)` falls back to a full pass — the bulk dumps are
quarterly snapshots, not incremental. The base class's storage layer
deduplicates against the existing `index.json`, so reruns are cheap
no-ops on previously-ingested opinion IDs.

## Why this exists

See [issue #549](https://github.com/ZachLaik/LegalDataHunter/issues/549).
Short version: our existing US case law layer is built on Harvard CAP
(frozen 2024) plus a half-empty constellation of state-court collectors.
CourtListener bulk solves both problems in one collector.

The companion follow-up is to add Juriscraper-backed daily updates for
state and federal courts (issue #549, Phase 2) — that's a separate PR.

## License

[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) — Free Law Project dedicates data to public domain.
