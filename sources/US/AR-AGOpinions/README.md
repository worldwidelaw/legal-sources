# US/AR-AGOpinions — Arkansas Attorney General Opinions

Full text of official legal opinions issued by the Arkansas Office of the
Attorney General. Each opinion answers a legal question posed by a public
official (legislator, agency head, prosecutor, etc.) and is an authoritative
(advisory) interpretation of Arkansas law — classified as **doctrine**.

## Source

- **Public search page:** https://arkansasag.gov/divisions/opinions-foia/attorney-general-opinions-search/
- **Search app (iframe):** https://prod.opinions-search.arkansasag.gov/ (React SPA)
- **Data backend:** public Meilisearch index `opinions` at
  `https://ms-20efc50b2abc-32991.sfo.meilisearch.io`
- **Coverage:** ~10,599 opinions (full text)
- **Auth:** none (read-only search key shipped in the public SPA bundle)

## How it works

1. Page through `GET /indexes/opinions/documents?limit=N&offset=M` on the
   Meilisearch backend using the public read-only search key.
2. Each document already carries the full opinion body in the `FullText`
   field, plus `OpinionSummary`, `RequestorNameFull`, `Date_Published`, and
   a `PDFUrl`. No per-document HTML scraping or PDF download is required.
3. Normalize into the standard doctrine schema (`text` holds the full
   opinion). Rows with no usable full-text layer are skipped.

## Usage

```bash
python bootstrap.py test-api            # connectivity + structure check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all opinions)
```

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Arkansas Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
