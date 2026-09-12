# AU/ACCC — Australian Competition and Consumer Commission (ACCC)

**Source:** [https://www.accc.gov.au/](https://www.accc.gov.au/)
**Data types:** doctrine
**Access:** Drupal JSON:API at `https://www.accc.gov.au/jsonapi` — no auth

## Coverage

~19,000+ full-text documents across the ACCC's content types:

| doc_type | node type | approx. count |
|---|---|---|
| media_release | `accc_news` | 8,300 |
| notification | `acccgov_notification` | 4,900 |
| merger_review | `acccgov_informal_merger_review` | 1,500 |
| undertaking | `acccgov_undertaking` | 1,400 |
| authorisation | `acccgov_authorisation` | 880 |
| update | `acccgov_update` | 750 |
| publication | `accc_publication`, `accc_serial_publication` | 660 |
| speech | `acccgov_speech` | 540 |
| project / guidance | `acccgov_project*`, `accc_page` | 150+ |
| merger_authorisation, class_exemption, public_register | misc. register types | small |

Full text is inline in the node's `field_accc_body` / `field_acccgov_body` /
`field_acccgov_summary` / `field_acccgov_speech_summary` fields — no detail
fetch or PDF extraction required.

## Access notes (the two traps)

1. **`page[offset]` / `page[limit]` are rejected site-wide** with HTTP 400
   `Input value "page" contains a non-scalar value` — even though the API
   itself emits those URLs in `links.next`. Pagination is therefore a
   **keyset walk**: `sort=drupal_internal__nid` plus
   `filter[ks][condition][path]=drupal_internal__nid&...[operator]=>&...[value]=<last nid>`.
   (Filter syntax is accepted; only `page` is not.)

2. **Full attribute requests return HTTP 503** for several types
   (`accc_news`, `accc_page`, `accc_publication`, `acccgov_undertaking`, …).
   A **sparse fieldset** (`fields[node--TYPE]=...`) avoids the failing render
   path. Drupal ignores field names a type does not define, so a single union
   field list works for every type.

Additionally the `/news-centre` HTML listing is now client-rendered and emits
no `/media-release/` hrefs, so media releases must come from `node--accc_news`
rather than HTML scraping.

Register index types with no body text (`acccgov_acquisition`,
`acccgov_acquisition_waiver`, `acccgov_public_register_document`,
`acccgov_project_document`, `acccgov_infringement_notice`) are not crawled —
their substance lives in attached PDFs.

## Usage

```bash
python bootstrap.py test                # connectivity + keyset check
python bootstrap.py bootstrap --sample  # 15 sample records
python bootstrap.py bootstrap-fast      # full corpus -> data/records.jsonl
python bootstrap.py update              # nodes changed since last run
```

`data/checkpoint.json` records the last nid reached per content type, so an
interrupted run resumes instead of re-appending the first N records.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — ACCC website
material is released under CC BY 4.0 (see
[accc.gov.au/copyright-notice](https://www.accc.gov.au/about-us/website/copyright-notice));
attribution required, commercial use permitted.
