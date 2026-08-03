# UK/SchoolsAdjudicator — Office of the Schools Adjudicator (OSA) Determinations

Determinations of the **Office of the Schools Adjudicator (OSA)** for England,
published on GOV.UK.

The Schools Adjudicator is an independent, quasi-judicial office established
under the **School Standards and Framework Act 1998**. Adjudicators decide:

- **Objections** to the admission arrangements of maintained schools and
  academies (s.88H) — e.g. oversubscription criteria, faith priority, catchment
  areas, PAN (published admission number);
- **Referrals / variations** to determined admission arrangements (s.88I / s.88E);
- **School-organisation disputes** (e.g. transfer of land, disposal of playing
  fields).

Their determinations are **binding on the admission authority concerned** and
are therefore case law (decisions on specific cases).

## Coverage

- ~2,240 determinations (2000s–present).
- Full text = the born-digital **determination PDF** attached to each entry.

## How it works

1. **Discovery** — GOV.UK Search API:
   `GET /api/search.json?filter_format=decision&filter_organisations=office-of-the-schools-adjudicator`
   paginated 200/page by `start`.
2. **Per-decision** — GOV.UK Content Store API `GET /api/content/{path}` gives
   the `content_id`, publication date, and the PDF attachment URL under
   `details.attachments` (`assets.publishing.service.gov.uk/media/...`).
   The Content Store `details.body` holds only a short summary, so the full
   determination is taken from the PDF.
3. **Full text** — the PDF is born-digital; text is extracted with the shared
   `common.pdf_extract` extractor (no OCR needed).

Case references (`ADA…`, `VAR…`, `STP…`, `REF…`) are parsed from the title or
the determination body where present.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; GOV.UK content published under OGL v3.0. Attribution required; commercial use permitted.
