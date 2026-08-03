# UK/TeachingRegulationAgency — Teacher Misconduct Panel Decisions

Teacher-misconduct professional-conduct panel decisions of the **Teaching
Regulation Agency (TRA)** for England, published on GOV.UK.

The TRA is an executive agency of the Department for Education that regulates
the teaching profession in England. Independent professional-conduct panels
hear allegations of:

- **Unacceptable professional conduct**;
- **Conduct that may bring the profession into disrepute**;
- **Relevant criminal convictions**.

On the panel's recommendation, the Secretary of State decides whether to
impose a **prohibition order** (a ban from teaching, with or without a review
period). Each published outcome sets out the panel's findings of fact,
reasons, and the final decision — quasi-judicial professional-discipline case
law.

## Coverage

- ~1,660 decisions.
- Full text = the born-digital **panel-decision PDF** attached to each entry.

## How it works

1. **Discovery** — GOV.UK Search API:
   `GET /api/search.json?filter_format=decision&filter_organisations=teaching-regulation-agency`
   paginated 200/page by `start`.
2. **Per-decision** — GOV.UK Content Store API `GET /api/content/{path}` gives
   the `content_id`, publication date, and the PDF attachment URL under
   `details.attachments` (`assets.publishing.service.gov.uk/media/...`).
   The Content Store `details.body` holds only a short notice/summary, so the
   full decision is taken from the PDF.
3. **Full text** — the PDF is born-digital; text is extracted with the shared
   `common.pdf_extract` extractor (no OCR needed). The `TRA reference` is
   parsed from the decision body where present.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; GOV.UK content published under OGL v3.0. Attribution required; commercial use permitted.
