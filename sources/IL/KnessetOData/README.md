# Israel Knesset OData API (Bills & Laws)

**Source:** [https://knesset.gov.il/Odata/ParliamentInfo.svc](https://knesset.gov.il/Odata/ParliamentInfo.svc)
**Country:** IL
**Data types:** legislation
**Status:** Complete

Israeli primary and secondary legislation with full Hebrew text, taken from the
Knesset's official OData service and the gazette (Reshumot) PDFs it links to on
`fs.knesset.gov.il`.

## Access path

Official OData v3 REST service — no key, no auth, no rate-limit headers.

```
https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_DocumentLaw?$top=100&$format=json
```

### Entity model

The service exposes two different identifier spaces, and confusing them is what
made an earlier version of this scraper enumerate nothing (issue #1438):

| Entity | Rows | Meaning |
|--------|------|---------|
| `KNS_IsraelLaw` | 2,024 | The **consolidated** law as an abstract, still-amended entity. Carries no documents. IDs from 2000001. |
| `KNS_Law` | 61,348 | The individual legislative **act**: an original law, an amendment, secondary legislation, a statutory report. IDs from 2001428. |
| `KNS_DocumentLaw` | 10,613 | Published files (`FilePath`), keyed by `KNS_Law.LawID`. ~7,200 are PDFs; the rest are `.tif` scans, `.doc`/`.docx`, `.xlsx`. |
| `KNS_LawBinding` | 15,326 | Links an act to its consolidated parent (`LawID` → `IsraelLawID`). |
| `KNS_DocumentIsraelLaw` | 0 | Looks like the missing join table; it is empty. |

`KNS_DocumentLaw.LawID` holds `KNS_Law` IDs, **not** `IsraelLawID` values.
Filtering it by an `IsraelLawID` returns `{"value": []}` with HTTP 200 — a
silent empty result, not an error.

The crawl is therefore document-first, which also guarantees every emitted
record has a file behind it:

```
KNS_DocumentLaw (FilePath) → KNS_Law (title, gazette date)
                           ↳ KNS_LawBinding → KNS_IsraelLaw (parent law)
```

## Hebrew text direction

pdfminer and pdfplumber emit these PDFs in **visual** order, so every Hebrew
line arrives character-reversed (`קוח` instead of `חוק`) and no downstream regex,
tokenizer or search can match it. PyMuPDF applies bidi reordering and returns
logical order, so it is the primary backend. The pdfminer fallback — for hosts
without PyMuPDF — reverses each Hebrew line back and flips the embedded
Latin/digit runs (dates, section numbers) the second time around.

## Usage

```bash
python3 bootstrap.py test                    # probe the OData service end to end
python3 bootstrap.py bootstrap --sample      # 15 records into sample/
python3 bootstrap.py bootstrap               # full corpus → data/records.jsonl
python3 bootstrap.py bootstrap-fast          # alias for the full corpus
python3 bootstrap.py update --since 2026-01-01
```

Incremental updates filter on `KNS_DocumentLaw.LastUpdatedDate`, which the
Knesset service touches whenever a file is (re)published.

## Record shape

Full text lands in `text`. Records also carry `document_id`, `law_id`,
`law_type` / `law_subtype`, `document_group`, `knesset_num`, the gazette
citation (`publication_series`, `gazette_number`, `gazette_page`) and the
consolidated-law link (`israel_law_id`, `israel_law_name`, `is_basic_law`,
`validity`).

`date` is the gazette publication date. A handful of administrative acts carry
no `PublicationDate`, so it falls back to the record's `LastUpdatedDate` rather
than emitting null.

## License

[Open Government Data (Israel)](https://main.knesset.gov.il/Activity/Info/Pages/Databases.aspx) — official Knesset open databases, attribution required. Commercial use permitted.
