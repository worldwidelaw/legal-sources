# CH/OpenCaseLaw

Swiss court decisions from OpenCaseLaw.ch / Entscheidsuche.

## Source

HuggingFace dataset: [voilaj/swiss-caselaw](https://huggingface.co/datasets/voilaj/swiss-caselaw)

## Data

- 1,060,259 court decisions from all Swiss cantons and federal courts
- Full text in German, French, Italian, Romansh
- Structured metadata: court, canton, docket number, legal area
- Citation references between decisions

`judges` and `outcome` are carried through by `normalize()` but are empty upstream at
every offset probed (0, 300K, 900K) — that is the dataset, not a mapping bug. `legal_area`
is populated only in part of the corpus (empty at offsets 0 and 300K, ~57% at 900K), so the
cantonal shards the samples come from show it null.

## Access

No authentication required.

The full crawl reads HuggingFace's parquet export — 115 shards, ~8GB, streamed and
deleted one shard at a time so peak disk stays under the largest shard (~1.3GB) and
peak memory under one row group (5,000 rows, ~138MB).

The datasets-server rows API is the fallback, used only if the parquet listing is
unavailable. It is correct but far too slow for this corpus: paging 1.06M full-text
rows 100 at a time delivered 559,850 of them before the fleet's 100h cap cut the run,
which is the coverage gap in #1505.

Both paths emit the same rows in the same order — verified against the rows API at
offsets 350 and 559,850 — so the row-offset checkpoint is valid for either, and a run
killed on one resumes on the other. `update` stays on the rows API, which pages the
tail directly.

## License

Data sourced from HuggingFace; original court decisions are public domain under Swiss law.

## Usage

```bash
python bootstrap.py test                 # Test connectivity
python bootstrap.py bootstrap --sample   # Fetch 15 samples
python bootstrap.py bootstrap-fast       # Full fetch (1.06M rows via the ~8GB parquet export)
python bootstrap.py bootstrap-fast --restart  # ...ignoring the resume pointer
python bootstrap.py update               # Incremental: only rows added upstream
```

## Checkpoints

`data/hf_checkpoint.json` holds two independent pointers:

- `full_sha` / `full_offset` — resume pointer for a full crawl still in flight, as a
  global row offset: written per shard on the parquet path, every 5,000 rows on the
  rows API. A killed run (100h cap, OOM, gateway error) resumes from here instead of
  re-walking from zero. Shards fully below the pointer are skipped on their parquet
  footer alone — a few KB over HTTP range requests, rather than downloading a shard to
  discard it. Only honoured while the dataset revision is unchanged; a new `sha` may
  rewrite existing rows, so the crawl restarts.
- `sha` / `rows_seen` — incremental pointer, advanced only by a walk that ran to
  completion. `update` compares the dataset `sha` first: unchanged means no work,
  at the cost of one request instead of re-walking 1.06M rows.

The two are deliberately separate. If a truncated crawl advanced `rows_seen`, the
next `update` would start past rows that may never have reached the database.
Leaving `rows_seen` behind instead degrades that run to a slow full walk, which
backfills rather than silently skips.
