# DK/Retsinformation — Denmark Retsinformation (Official Law Database)

**Source:** [https://www.retsinformation.dk/](https://www.retsinformation.dk/)
**Data types:** legislation

## Access

Discovery walks the 21-page sitemap index (~198K ELI URLs, newest-first). Each
entry carries a `<lastmod>`, which is what `--since` filters on for an
incremental refresh. Completed ELI paths are checkpointed to
`data/eli_checkpoint_v2.txt` so an interrupted full walk resumes.

Full text comes from two endpoints, tried in this order:

1. `GET {eli_url}/xml` — the official ELI XML. Text is the non-`<Meta>`
   children of `<Dokument>`.
2. `POST /api/document/eli/{path}` with `{"isRawHtml": false}` — the endpoint
   the retsinformation.dk SPA itself uses, returning `documentHtml`.

The second path is not optional. Roughly the older half of the sitemap serves a
Meta-only XML stub (or 404s on `/xml`) while the full text is only reachable via
the document API — a sampled 120 documents from that region yielded 0/120 on the
XML path alone and 120/120 with the fallback. Relying on `/xml` silently dropped
~109K documents ([#1549](https://github.com/ZachLaik/LegalDataHunter/issues/1549)).
The fallback sits *below* the XML path, so documents the XML path already handles
re-extract byte-identically.

Every dropped document logs a `SKIP {key}: {reason}` line, and the progress
counter carries a reason breakdown, so a future coverage cliff is visible in the
log rather than hidden behind a bare skip count.

## License

Public domain — Danish Copyright Act § 9
