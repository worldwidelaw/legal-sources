# AU/QLD-Legislation — Queensland Legislation (legislation.qld.gov.au)

**Source:** [https://www.legislation.qld.gov.au/](https://www.legislation.qld.gov.au/)
**Data types:** legislation

Queensland Acts and subordinate legislation published by the Office of the
Queensland Parliamentary Counsel (OQPC), in QuILLS DTD XML.

## Coverage

| Rendition | URL segment | Documents |
|---|---|---|
| Acts in force | `inforce/current` | 574 |
| Subordinate legislation in force | `inforce/current` | 433 |
| Repealed Acts | `repealed` | 485 |
| Repealed subordinate legislation | `repealed` | 1,408 |
| Acts as passed | `asmade` | 6,629 |
| Subordinate legislation as made | `asmade` | 11,472 |

The renditions are crawled in that order, so a truncated run still lands
current law first.

## How it works

1. **Discovery** — every document ID comes from `/projectdata`, the browse
   data API the portal's own UI calls:

   ```
   GET /projectdata?ds=OQPC-BrowseDataSource&collection=OQPC.toc
       &start=1&count={total}
       &expression=Repealed=N AND PrintType=act.reprint AND PitValid=@pointInTime({server_time})
   ```

   `start` is **1-based** — passing `0`, or omitting it, returns
   `totalCount`/`filteredCount` with no `data` array. `{server_time}` is the
   `data-server-time` attribute on `/browse/inforce`, which is what the browse
   UI feeds to `@pointInTime`. One request returns a whole rendition.

2. **Full text** — `GET /view/whole/xml/{rendition}/{doc_id}`, tags stripped
   from the QuILLS XML.

## Gotchas

- A **browser User-Agent trips the site's Imperva JS challenge** (HTTP 302 to a
  "Loading" interstitial). The plain `LegalDataHunter/1.0` identity is served
  normally — do not "fix" the UA.
- **Do not enumerate the ID space.** Many instruments have non-numeric IDs
  (`act-1914-bsga`, `act-1995-lra`, `act-1889-acadbpa`) that an
  `act-YYYY-NNN` sweep can never reach, and the sweep costs ~133,000 requests
  to find ~1,000 documents. IDs are also mixed case (`SL-1998-0058`) — pass
  them through verbatim.
- As-passed/as-made texts share an instrument ID with their in-force reprint,
  so their record `_id` carries a `:asmade` suffix.
- Every request has a wall-clock deadline on top of the socket timeout, and
  completed documents are checkpointed to `data/qld_checkpoint.txt`, so a
  restart resumes instead of re-walking (issue #1373).

## Usage

```bash
python bootstrap.py test             # connectivity + enumerate all renditions
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap-fast   # full crawl (fleet entry point)
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required.
