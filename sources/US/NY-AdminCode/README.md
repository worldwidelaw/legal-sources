# US/NY-AdminCode — New York Codes Rules and Regulations (NYCRR)

**Source:** [https://www.law.cornell.edu/regulations/new-york](https://www.law.cornell.edu/regulations/new-york)
**Data types:** legislation

Full text of every NYCRR section across the 24 active titles (~30K sections),
read from Cornell LII. The official publisher's site (`govt.westlaw.com/nycrr`)
sits behind a Cloudflare challenge, so LII is the usable full-text mirror.

## How the crawl works

1. `/regulations/new-york` lists the 24 titles.
2. Each title is walked breadth-first through chapter → subchapter → part →
   subpart. Section links are collected at **every** level, not just the first
   one that has any: a part page routinely lists its own sections *and* links
   deeper named subdivisions carrying more
   (`title-6/chapter-I/subchapter-A/part-1` has §§1.10–1.13, while
   `.../part-1/bear` has §§1.31–1.32).
3. Each section page is fetched and the regulation body extracted, with LII's
   navigation, version-comparison widget, and footer stripped.

## Resumability

The walk is ~34K pages, so a single run can be cut short. State lives under
`data/`:

- `enumeration.json` — per-title section lists, written only when that title's
  enumeration finished; a partial walk is never cached as final.
- `done_sections.txt` — sections already written, appended as they land.
- `section_stamps.json` — the upstream `Last-Modified` of each section page as we
  last read it; the comparator an incremental refresh comes back with.

A restart reuses all three and fetches only what is pending.

## Incremental refresh

NYCRR carries no amendment date to filter on, and Cornell publishes no sitemap,
no feed and no "recently updated" listing — there is no index that would let a
refresh skip pages without asking about them. What Cornell does serve is a
per-page `Last-Modified` and working conditional GET, and that is the comparator
`fetch_updates` uses: each section is requested with `If-Modified-Since` set to
the stamp of the copy we hold, so an unchanged page comes back **304 with an
empty body** instead of 27KB–340KB of HTML to re-parse and re-upsert. Verified
2026-08-30: 6/6 sample sections answered 304 against their stored stamps.

The comparison is per section, not per run: asking "has *this* page moved since
my copy" stays correct even if the fleet's `last_run` bookkeeping drifts. A
section with no stored stamp falls back to `since` when it is in
`done_sections.txt` (we demonstrably read it before that run) and is fetched
unconditionally otherwise — a section we do not actually hold can never be
skipped.

**Not pruned at the hierarchy level.** A part page's `Last-Modified` *lags* its
own sections' by seconds — measured 2026-08-30, `part-500` at 20:54:28 vs its
newest section at 20:54:37 — so skipping a subtree whose index page looks
untouched would silently drop real updates. The hierarchy is re-walked in full
on every refresh, which is also what surfaces sections Cornell added since the
last crawl.

Because these stamps are the only date Cornell exposes, the record's `date` field
now carries the upstream rebuild date rather than the crawl timestamp it used to.

## Usage

```bash
python bootstrap.py test-api             # connectivity check
python bootstrap.py bootstrap --sample   # ~15 sample sections
python bootstrap.py bootstrap            # full run → data/records.jsonl
python bootstrap.py bootstrap-fast       # full run, concurrent normalize
python bootstrap.py update               # conditional-GET refresh since last_run
```

Environment overrides:

| Variable | Default | Purpose |
|---|---|---|
| `NY_ADMINCODE_CRAWL_GAP` | `1.0` | Seconds between requests, shared across the pool |
| `NY_ADMINCODE_WORKERS` | `4` | Section-fetch pool size |
| `NY_ADMINCODE_REQUEST_DEADLINE` | `90` | Wall clock per request before it is abandoned |
| `NY_ADMINCODE_MAX_RUNTIME` | `72000` | Wall clock for the whole run (`0` disables) |

Requests are paced to roughly 1/s in aggregate rather than the 10s
`Crawl-delay` in LII's `robots.txt`, which would put a single full pass at ~83
hours; the run is bounded and resumable instead.

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)

NYCRR text is a New York State government edict of government and is not
subject to copyright. Cornell LII hosts it as a public mirror.
