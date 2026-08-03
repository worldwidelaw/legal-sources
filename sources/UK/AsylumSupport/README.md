# UK/AsylumSupport — First-tier Tribunal (Asylum Support) Decisions

Decisions of the **First-tier Tribunal (Social Entitlement Chamber) — Asylum
Support**, published on GOV.UK. The tribunal hears appeals by asylum seekers
and failed asylum seekers against Home Office decisions to refuse or
discontinue asylum support and accommodation under **sections 95, 98 and 4 of
the Immigration and Asylum Act 1999**.

- **Coverage:** ~101 decisions
- **Type:** case_law
- **Full text:** Content Store `details.metadata.hidden_indexable_content`
  (born-digital, clean plain text, no OCR); for the minority of records whose
  `hidden_indexable_content` is empty, a born-digital decision **PDF attachment**
  is downloaded and extracted via `common.pdf_extract` as a fallback.

## How it works

Uses the shared GOV.UK finder + Content Store recipe (as UK/RPT, UK/ET,
UK/FTT-Tax, UK/TrafficCommissioner):

1. **Discovery** — `GET /api/search.json?filter_format=asylum_support_decision`
   (paginated).
2. **Full text** — `GET /api/content/{path}`; text from
   `details.metadata.hidden_indexable_content`.

```bash
python bootstrap.py bootstrap --sample   # 12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

Rich metadata is captured: categories, sub-categories, judges, reference
number and decision date.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
— Crown copyright; GOV.UK content published under OGL v3.0. Attribution
required; commercial use permitted.

## Related sources

- **UK/IAC** — Immigration and Asylum Chamber (Upper Tribunal immigration
  appeals) — distinct jurisdiction.
- **UK/RPT**, **UK/ET**, **UK/FTT-Tax**, **UK/TrafficCommissioner** — sibling
  GOV.UK finder scrapers.
