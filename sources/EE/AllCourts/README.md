# EE/AllCourts — Estonian Court Decisions (Kohtulahendid, All Courts)

Full text of decisions of **all Estonian courts**, published on the official
**Riigi Teataja** (State Gazette of Estonia):

- County courts (*maakohus*) — civil & criminal
- Administrative courts (*halduskohus*)
- Circuit courts of appeal (*ringkonnakohus*)
- Older Supreme Court (*Riigikohus*) material (also covered by `EE/SupremeCourt`)

Each published decision (*kohtuotsus* / *kohtumäärus*) finally adjudicates a
specific case — this is **case_law**.

- **Corpus:** ~722,556 decisions (2006–present for lower courts)
- **Language:** Estonian (`et`)
- **Auth:** none (open public API)

## Access

Riigi Teataja is an Angular SPA backed by an **open public JSON API**:

- **Search** — `POST /public-api/api/v1/kohtuteave/otsing/kohtulahendid`
  ```json
  {"general": {"searchText": "", "sort": "LahendiKuulutamiseAeg",
               "sortAscending": true, "searchAfter": 0}, "precise": {}}
  ```
  returns `{"kokku": 722556, "tulemused": [ …30 decisions… ]}`.
  `searchAfter` is a numeric **offset** (increments of 30); the scraper walks it
  ascending (oldest first) and checkpoints to `data/checkpoint.json`, so fleet
  reruns resume and eventually complete the full corpus.
- **Full text** — `GET /public-api/api/v1/kohtuteave/kohtulahendid/{avalikustatudFailiId}/file`
  is the **born-digital PDF**. Send `Accept: application/pdf` — the API returns
  HTTP 406 for `Accept: application/json`. Extracted with PyMuPDF (fitz); no OCR.

### History

Previously blocked as `no_full_text_access`: the **old** `kohtulahendid.ee` /
`fail.html?fid=X` endpoint served full text behind a Cloudflare JS challenge.
The rebuilt Riigi Teataja SPA exposes the open API above, which unblocks it.

## Usage

```bash
python bootstrap.py test                # connectivity + one sample
python bootstrap.py bootstrap --sample  # 15 validation samples
python bootstrap.py bootstrap-fast      # full corpus (checkpointed, resumable)
python bootstrap.py update              # incremental (newest decisions)
```

## License

[Public Domain — official documents](https://www.riigiteataja.ee/en/eli/512122024001/consolide) — Estonian Copyright Act (*Autoriõiguse seadus*) **§5** excludes court decisions and other official documents from copyright protection. Commercial use permitted; no attribution required. Published on the official Riigi Teataja.
