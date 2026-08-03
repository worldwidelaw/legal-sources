# UK/LegalOmbudsman — Legal Ombudsman (LeO) Public Interest Decisions

Published **public interest decisions** of the **Legal Ombudsman (LeO)**, the
statutory ombudsman scheme run by the Office for Legal Complaints (established
under the Legal Services Act 2007) that resolves complaints about legal service
providers in England & Wales.

Under LeO's *policy statement on publishing our decisions*, the Ombudsman
publishes selected final decisions in the public interest — full, reasoned
determinations that name the legal service provider. These are `case_law`.

## Source

- **Listing:** https://www.legalombudsman.org.uk/information-centre/public-interest-decisions/
  — a single page with one card per published decision.
- **Full text:** each card links to a born-digital decision PDF at
  `/media/{id}/{slug}-pid.pdf`, extracted via `common.pdf_extract` (no OCR).
- **Date:** stamped in the PDF body (`Final Decision / Date DD Month YYYY`).

Coverage: ~25–30 published public-interest decisions (a curated, growing set).

**Not** LeO's firm-level "ombudsman decision data" CSV — that is aggregate
complaint-outcome metadata (firm name + counts), with no decision text, so it is
deliberately excluded here.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # 12 sample records for validation
python bootstrap.py bootstrap-fast       # Alias for full pull (fleet runner)
```

## Record schema

| Field   | Description                                        |
|---------|----------------------------------------------------|
| `_id`   | `UK/LegalOmbudsman/{slug}`                          |
| `_type` | `case_law`                                          |
| `title` | `{firm} — Legal Ombudsman public interest decision` |
| `text`  | Full decision text (from PDF)                       |
| `date`  | Final decision date (ISO 8601)                     |
| `firm`  | Named legal service provider                        |
| `summary` | LeO's published insight/summary for the decision |
| `url`   | Decision PDF URL                                    |

## License

> ⚠️ **Commercial use restricted.** No Open Government Licence applies; treat as
> custom terms pending confirmation.

[Policy statement on publishing our decisions](https://www.legalombudsman.org.uk/information-centre/data-centre/ombudsman-decision-data/policy-statement-publishing-our-decisions/)
— Legal Ombudsman / Office for Legal Complaints. Public-interest decisions
published for transparency; no explicit open licence, attribution expected.
