# IL/CaseLawHF — Israeli Case Law (HuggingFace)

Israeli court judgments from the HuggingFace dataset [guychuk/case-law-israel](https://huggingface.co/datasets/guychuk/case-law-israel).

- **Rows**: 10,558 — of which roughly half carry judgment text (see *Coverage* below)
- **Language**: Hebrew
- **Courts**: Supreme, District, Magistrate, Family, Local Affairs, Traffic, Small Claims, Juvenile, Labour, Parole Board
- **Districts**: Northern, Haifa, Tel Aviv, Central, Jerusalem, Southern
- **Full text**: Yes — `document_text` holds the complete judgment

## Coverage

About 48% of the dataset's rows have an empty `document_text`. Those rows carry only
PDF *metadata* in `files_str` (file name, mime type, byte size) with no bytes and no
retrievable URL, so the text is not recoverable from this dataset. `normalize()`
returns `None` for them and they are skipped, which is why an ingested count near
~5,500 is expected rather than 10,558.

## Court metadata

`court_type_code`, `district_code` and `publication_subject_code` are `ClassLabel`
columns, so the dataset publishes its own authoritative code→name lists in its
feature schema. `bootstrap.py` mirrors those lists verbatim, and `bootstrap.py test`
re-reads the live schema and fails if it has drifted.

Each record carries a per-record `court`, `court_tier` and `court_id`. The tier
convention matches the rest of the repo (1 = apex, 2 = appellate, 3 = first
instance). A tier is emitted only where the court type determines it:

| Court | Tier |
|---|---|
| Supreme Court of Israel | 1 |
| District Court | 2 |
| National Labour Court, District Juvenile Court | 2 |
| Magistrate, Family, Local Affairs, Traffic, Small Claims, Juvenile, Regional Labour | 3 |
| Parole Board | `null` (administrative tribunal, outside the court hierarchy) |
| Unrecognised / `UNKNOWN` code | `null` |

Labour judgments share one court code across the regional (first instance) and
national (appellate) benches, so the bench is read from the judgment's own title
(`הארצי` vs `אזורי`); when neither appears, the tier stays `null`.

## URL provenance

This corpus has **no verified per-judgment link on any official Israeli court
site**, and the dataset's `url_name` is an internal slug, not a resolvable path.
Records therefore carry:

- `url` — a deep link into the HuggingFace dataset viewer for this judgment
- `url_provenance` — `"dataset_viewer"`, i.e. an aggregator link, not a publisher link
- `publisher_url` — always `null` (fails closed)
- `publisher_ref` — the raw `url_name` slug, for traceability only

Before issue #1533 the slug was expanded into `https://www.nevo.co.il/psika_word/{url_name}`
and emitted as `url`, which presented a constructed link as publisher-provided.
That path does not exist (404 to a browser, 403 otherwise) and Nevo is a
commercial paid database, so it is never reconstructed.

## License

Unverified — sourced from Israeli court records. No explicit license on HuggingFace dataset card. Israeli court decisions are generally public domain as government works.
