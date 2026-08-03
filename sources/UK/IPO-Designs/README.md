# UK/IPO-Designs — IPO Registered Design Hearing Decisions

Hearing decisions of the **UK Intellectual Property Office (IPO)** concerning
**registered designs** — invalidation, entitlement and related disputes decided
by an IPO Hearing Officer under the **Registered Designs Act 1949**. Published on
GOV.UK and reachable without browser automation (unlike the Cloudflare-gated
`ipo.gov.uk` decisions database, tracked separately as the blocked source
**UK/IPO**).

- **Coverage:** ~134 decisions
- **Type:** case_law
- **Full text:** born-digital decision **PDF attachment** on
  `assets.publishing.service.gov.uk`, extracted via `common.pdf_extract`
  (no OCR needed)

## How it works

Uses the shared GOV.UK finder + Content Store recipe (as UK/SchoolsAdjudicator,
UK/RPT, UK/ET):

1. **Discovery** — `GET /api/search.json?filter_format=design_decision`
   (paginated).
2. **Metadata + PDF** — `GET /api/content/{path}`; the full decision is the PDF
   attachment (`details.attachments`). `hidden_indexable_content` is empty for
   this format, so the PDF is downloaded and extracted.

```bash
python bootstrap.py bootstrap --sample   # 12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

Metadata captured: British Library decision number (e.g. `O/0543/26`), decision
date, hearing officer and litigants.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
— Crown copyright; GOV.UK content published under OGL v3.0. Attribution
required; commercial use permitted.

## Related sources

- **UK/IPO** — the main IPO patent/trademark/design decisions database on
  `ipo.gov.uk` (blocked: Cloudflare managed challenge). This source covers the
  design decisions that IPO also mirrors on GOV.UK.
- **UK/SchoolsAdjudicator**, **UK/RPT**, **UK/ET**, **UK/TrafficCommissioner** —
  sibling GOV.UK finder scrapers.
