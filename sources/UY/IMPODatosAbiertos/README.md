# UY/IMPODatosAbiertos — Uruguay IMPO Open Data JSON API

**Source:** [https://www.impo.com.uy/datos-abiertos/](https://www.impo.com.uy/datos-abiertos/)
**Data types:** legislation

Full text of Uruguayan executive-branch normativa from IMPO (Centro de Información
Oficial). Any public document URL returns JSON when `?json=true` is appended.
Laws (*leyes*) are covered by `UY/IMPO` via Parlamento; this source covers the
complementary types:

| Type | Path | Coverage |
|------|------|----------|
| Constitution | `constitucion/1967-1967` | 1967 text (~213K chars) |
| Decrees | `decretos/{n}-{year}` | 1964–present, ~500–700 per year |
| Decree-Laws | `decretos-ley/{n}-{year}` | 1973–1985, numbered in the national law series (~14,100–15,850) |
| Executive Resolutions | `resoluciones/{n}-{year}` | 1975–present, sparse numbering up to ~1,500 |

## Enumeration

IMPO publishes no listing or index endpoint, so the corpus is enumerated by
probing the (document number, year) space. A miss is served as an HTML
"Acceso no válido" page rather than JSON, which is how invalid combinations are
detected.

- Numbers are probed in chunks of 100 by a bounded thread pool (8 workers).
- A year is abandoned only after a long unbroken run of misses (150 for decrees,
  300 for resolutions) — resolution numbering has gaps well over 100, so a small
  tolerance silently truncates the corpus.
- Decree-laws continue the national law number series instead of restarting each
  year, so the year is guessed per number from an empirically probed anchor grid
  and the nearest few years are tried.
- Every completed `(type, year)` unit is recorded in `data/scan_checkpoint.json`;
  a relaunch skips completed units with no network calls, so fleet re-runs
  advance monotonically instead of re-walking the whole space.

Records stream to `data/records.jsonl` through `BaseScraper`'s storage layer.

## Incremental refresh

`fetch_updates(since)` narrows on **`fechaPublicacion`** — the Diario Oficial issue
that carried the norm. IMPO is the official publisher, so a norm appears in these
databases on the day it is published: publication date is an availability stamp,
not a document date lagging behind it. It is also the only date the API exposes —
the JSON endpoint sends no `Last-Modified` and no `ETag`, and the payload carries
no modified stamp, so conditional GET and a modified-date facet are both out.

Two narrowings, and both are needed:

- **Publication cutoff** decides what is *emitted*. A refresh upserts the norms
  published since `since` instead of every norm of the last two years.
- **Per-unit high-water number** decides where the probe *starts*. Numbering runs
  forward through the year, so anything published after `since` sits above the
  ceiling recorded on the last crawl. The mark is written to
  `data/scan_checkpoint.json` during any scan; for years crawled before the mark
  existed it is seeded by a strided descending probe (~36 requests) rather than by
  re-walking the year. That probe can only under-estimate, which costs extra
  probes but never drops a document.

Two safety margins keep the narrowing honest. Publication lags promulgation by
weeks and routinely crosses the new year, so the window never narrows past the
previous year even when `since` is yesterday. And numbering tracks publication
order only loosely — decreto 5/1985 was published two weeks *after* decreto
50/1985 — so the probe restarts 200 numbers below the high-water mark.

Decree-laws (1973–1985) and the 1967 Constitution are closed historical sets and
are skipped by a refresh; only decrees and resolutions still accrue numbers.

Measured on 2026-08-30 against a corpus crawled through 2025, `since=2026-08-01`:
598 documents probed, **54 emitted, 544 filtered as already-published**, 221s.

Not covered: IMPO regenerates a consolidated text when a norm is later amended,
and exposes no stamp for that. The amending norm is itself picked up as a new
publication, but the amended text of the older norm is only refreshed by a full
re-crawl.

## Usage

```bash
python bootstrap.py test-api              # connectivity check
python bootstrap.py bootstrap --sample    # ~15 curated samples to sample/
python bootstrap.py bootstrap             # full corpus → data/records.jsonl
python bootstrap.py bootstrap-fast        # same, fleet entry point
python bootstrap.py update                # incremental refresh since last_run
```

## License

Open government data — [https://www.impo.com.uy/datos-abiertos/](https://www.impo.com.uy/datos-abiertos/)
