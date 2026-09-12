# INTL/Juricaf — Francophone Supreme Court Decisions

**Source:** [Juricaf](https://juricaf.org)
**Operator:** AHJUCAF (Association des Hautes Juridictions de Cassation des pays ayant en partage l'usage du Français)
**Data type:** Case law
**Coverage:** ~1.86M decisions from 48 francophone jurisdictions

## Description

Juricaf is the largest free database of francophone supreme court decisions,
covering cassation and supreme courts from 48 countries and international
institutions. Created in 2005 with support from the International Organization
of the Francophonie (OIF) and the French Ministry of Justice.

### Countries covered

France (1.7M), Switzerland (54K), Luxembourg (28K), CJEU (25K), Senegal (12K),
Belgium (11K), ECHR (9K), Canada (5K), Monaco (5K), Benin (4K), Madagascar (3K),
Morocco (3K), OHADA (1.3K), Mali (1K), Niger (821), Cameroon (515), Chad (462),
Romania (497), Burkina Faso (210), Côte d'Ivoire (174), Togo (243), Bulgaria (136),
Congo (131), Guinea (128), CEMAC (121), Haiti (121), ECOWAS (113), Gabon (109),
UEMOA (95), DR Congo (89), Mauritania (58), Czech Republic (59), Cambodia (66),
CAR (53), Lebanon (35), Andorra (29), Tunisia (28), Burundi (25), Comoros (10),
and others.

## How the scraper works

Decision URLs come from the published sitemap: `/sitemap.xml` fans out to 38
chunks of up to 50,000 `<loc>` entries (~1.85M decisions), each with a
`<lastmod>` that drives `fetch_updates`. Full text is then read from the
`<article id="textArret">` element of each `/arret/{slug}` page.

Search pagination (`/recherche/+/facet_pays:{COUNTRY}?page=N`) is deliberately
**not** used: `robots.txt` disallows `?page=` and `?tri=` for `User-agent: *`,
and at 10 results per request it cost ~186,000 requests just to learn the URLs.

At the configured 1 request/second a full pass takes far longer than one fleet
slot, so the run is designed to be resumed rather than restarted:

- `data/checkpoint.json` records which sitemap chunks are finished and the
  offset reached inside the current one, saved every 25 URLs.
- Any decision already in storage is skipped without a request, so a resumed
  run (or a drifted sitemap offset) costs a local lookup rather than a refetch.
- The trailing chunk is never retired, since new decisions are appended to it.

Juricaf carries metadata-only stubs for some older decisions — mostly 19th
century Conseil d'État entries whose `<article>` is empty upstream. These are
skipped rather than stored as empty records.

## License

[Public Domain (Government Judicial Decisions)](https://juricaf.org/static/mentions_legales) — Supreme court decisions are public domain in most jurisdictions. AHJUCAF provides free access.
