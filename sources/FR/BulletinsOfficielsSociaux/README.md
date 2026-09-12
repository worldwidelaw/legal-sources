# FR/BulletinsOfficielsSociaux

**Bulletins officiels des ministères chargés des affaires sociales**
(Santé – Protection sociale – Solidarités & Travail – Emploi – Formation professionnelle)

- **URL:** https://bulletins-officiels.social.gouv.fr/
- **Country:** France (FR)
- **Data types:** legislation, doctrine
- **Auth:** none

## What this source covers

The official bulletins of the French social-affairs ministries. The single site
publishes two bulletins:

- **BO Santé – Protection sociale – Solidarités**
- **BO Travail – Emploi – Formation professionnelle**

Published acts include circulaires, instructions, arrêtés, décisions, notes
d'information, avis, conventions and délibérations. Binding regulatory acts
(arrêtés, décrets, décisions…) are tagged `legislation`; explanatory/instructional
texts (circulaires, instructions, notes, guides…) are tagged `doctrine`.

## How it works

The site is a Drupal install that exposes each node as JSON via the
`?_format=json` query parameter. The scraper:

1. Reads `/sitemap.xml` to enumerate every node path.
2. Fetches `<url>?_format=json` for each.
3. Keeps only real legal documents (nodes carrying a `field_institutional`
   document-type taxonomy term); utility/section pages are skipped.
4. Reads the full text from `field_body_text` (HTML stripped) and derives the
   date, document type, NOR reference and issuer from structured fields.

No PDF extraction is required for the binding body — annexes are linked PDFs but
the act text itself is in `field_body_text`.

If the sitemap is unreachable or returns nothing, the scraper falls back to
sweeping Drupal node ids (`/node/{nid}?_format=json`), which enumerates the same
corpus independently. Verified 2026-09-09: sitemap → 874 paths, node-id sweep →
877 nodes, both yielding the same 872 documents.

### Failing loud (issue #1599)

Node fetches are separated from content decisions. A page that is fetched fine
but carries no `field_institutional` term is a legitimate skip; an HTTP error,
a WAF interstitial or a connection failure is a *lost document* and raises
`NodeFetchError`. If more than half of the completed fetches fail, the crawl
aborts and the CLI exits non-zero.

This matters because the first fleet run wrote 60 of 874 documents and still
exited 0: every failed fetch was swallowed by `return None`, so a 93% loss was
indistinguishable from a small corpus. Requests now go through the shared
`HttpClient` (retries 429/5xx with capped backoff, honours `Retry-After`) paced
by an `AdaptiveRateLimiter`, so throttling slows the crawl instead of shredding
it.

Covers public-repo source requests **#1036** (Ministères chargés des affaires
sociales) and **#1037** (Ministère de la Santé — BO Santé).

## Usage

```bash
python bootstrap.py test               # connectivity test
python bootstrap.py bootstrap --sample # fetch sample records
python bootstrap.py bootstrap-fast     # full pull (VPS)
python bootstrap.py update             # re-scan sitemap (idempotent via Neon)
```

## License

[Licence Ouverte 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — French open licence. Commercial use permitted; attribution required.
