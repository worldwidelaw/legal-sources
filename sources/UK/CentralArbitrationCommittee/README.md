# UK/CentralArbitrationCommittee — Central Arbitration Committee (CAC) Decisions

Decisions of the **Central Arbitration Committee (CAC)**, the permanent
independent statutory body established under the Trade Union and Labour
Relations (Consolidation) Act 1992. The CAC adjudicates:

- statutory trade-union **recognition and derecognition** applications
  (Schedule A1 — acceptance, validity, bargaining-unit, ballot, declaration of
  recognition, and method decisions);
- **disclosure-of-information** complaints for collective bargaining;
- **European Works Council** and **Information & Consultation** disputes.

Each case produces one or more binding, reasoned decisions = quasi-judicial
case law.

## Data source

- **Discovery:** GOV.UK Search API
  `GET /api/search.json?filter_format=decision&filter_organisations=central-arbitration-committee`
  (~936 case pages, paginated `count`/`start`).
- **Full text:** GOV.UK Content Store API `GET /api/content/{path}` exposes each
  case's decision documents under `details.attachments`:
  - **older cases** attach born-digital decision **PDF(s)** on
    `assets.publishing.service.gov.uk` → extracted via `common.pdf_extract`
    (no OCR needed);
  - **newer cases** attach **`html_publication`** documents
    (`/government/publications/{slug}/{decision-slug}`) → fetched from the
    Content Store, `details.body` HTML stripped to text.
- All decision documents for a case are concatenated into one full-text record.
  `application-progress` tracker attachments are skipped (status, not decision).

No authentication required.

## Usage

```bash
python bootstrap.py bootstrap          # Full initial pull
python bootstrap.py bootstrap --sample # 12 sample records for validation
python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
```

## Output schema (normalized)

| field        | description                                        |
|--------------|----------------------------------------------------|
| `_id`        | `UK/CentralArbitrationCommittee/{content_id}`      |
| `_source`    | `UK/CentralArbitrationCommittee`                   |
| `_type`      | `case_law`                                          |
| `title`      | CAC outcome title (parties)                         |
| `text`       | full text of all decision documents for the case    |
| `date`       | first-published date (ISO 8601)                     |
| `url`        | GOV.UK case page                                    |
| `ref_url`    | first decision document URL (PDF or HTML)           |

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; GOV.UK content published under OGL v3.0. Attribution required; commercial use permitted.
