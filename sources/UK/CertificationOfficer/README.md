# UK/CertificationOfficer — Certification Officer Decisions

Decisions of the **Certification Officer** for Trade Unions and Employers'
Associations (Great Britain), published on GOV.UK.

The Certification Officer is the independent statutory regulator of trade
unions and employers' associations in Great Britain, operating under the
**Trade Union and Labour Relations (Consolidation) Act 1992**. The Officer
determines complaints brought by members and enforces statutory duties,
covering:

- **Breach of union rules** (discipline, expulsion, disciplinary procedures);
- **Elections** to union office and **industrial-action / political-fund ballots**;
- **Accounting records** and the register of members;
- **Amalgamations, transfers and mergers**;
- **Jurisdictional sift** decisions.

The decisions are quasi-judicial and binding.

## Coverage

- ~530 decisions.
- Full text = the born-digital **decision PDF(s)** attached to each entry.

## How it works

1. **Discovery** — GOV.UK Search API:
   `GET /api/search.json?filter_format=decision&filter_organisations=certification-officer`
   paginated 200/page by `start`.
2. **Per-decision** — GOV.UK Content Store API `GET /api/content/{path}` gives
   the `content_id`, publication date, and the PDF attachment URL(s) under
   `details.attachments` (`assets.publishing.service.gov.uk/media/...`).
   The Content Store `details.body` holds only a short summary, so the full
   decision is taken from the PDF(s).
3. **Full text** — the PDF(s) are born-digital; text is extracted with the
   shared `common.pdf_extract` extractor (no OCR needed). Where an entry has
   more than one attachment, the extracted texts are concatenated.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (fleet runner)
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; GOV.UK content published under OGL v3.0. Attribution required; commercial use permitted.
