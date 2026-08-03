# UK/GDC — General Dental Council — Fitness to Practise hearing determinations

The **General Dental Council (GDC)** is the UK statutory regulator for ~120,000
dentists and dental care professionals (dental nurses, hygienists, therapists,
technicians, clinical dental technicians, orthodontic therapists). Fitness to
practise and registration cases brought by the GDC are heard by the independent
**Dental Professionals Hearings Service (DPHS)**. Its committees — the
Professional Conduct Committee, Professional Performance Committee, Health
Committee, Interim Orders Committee — and the Registration Appeals panels sit
under the **Dentists Act 1984** and the **GDC (Fitness to Practise) Rules 2006**.

Each concluded hearing publishes a reasoned **"PUBLIC DETERMINATION"** setting
out the charges/allegation, the facts found proved, whether the registrant's
fitness to practise is impaired, and the sanction/order imposed (erasure,
suspension, conditions, reprimand) or the interim order made. These are binding
professional-regulator adjudications = **case law**, distinct from UK/GMC
(doctors), UK/SDT (solicitors), UK/BTAS (barristers), UK/HCPTS (health & care
professions), UK/NMC (nurses/midwives) and UK/SocialWorkEngland.

## Access & structure (all public, no auth)

`dentalhearings.org` is a Nuxt/Vue single-page app backed by a plain JSON API at
`https://api.dentalhearings.org`:

- `GET /Hearing?futureHearings=false&page={N}` — paginated list of concluded
  hearings (20/page, ~630 total across ~32 pages); each row carries `hearingId`,
  `name`, `registrationNumber`, `profession`, `hearingDate`, `hearingType`,
  `outcomeSummary`.
- `GET /Hearing/{hearingId}` — full detail incl. `determinationDocuments` and
  `chargeDocuments` arrays (each with `annotationId`, `filename`, `mimeType`).
- The PDFs are served from public Azure blob storage:
  `https://gdcolrlive1.blob.core.windows.net/annotationspublic/{annotationId}`.
  These are **born-digital** PDFs with a real text layer (no OCR): a structured
  header (committee / hearing type / dates / name / registration number / case
  number / representation / fitness to practise / outcome / immediate order /
  committee members) followed by the numbered reasoned determination.

The API exposes a rolling window of published concluded hearings (older ones are
removed under the GDC publication policy), so one run captures the current
window (~500-600 determinations) and re-runs accumulate the record (the pipeline
dedups on `_id` = the stable `hearingId`). The reasoned determination PDFs are
preferred; the **charge / Notice of Hearing** PDFs are used as a fallback when a
determination has not yet been published (very recent hearings).

## Output

Normalized `case_law` records with full determination text plus metadata:
registrant name, registration number, profession, hearing type, outcome,
document kind (`determination` / `charge`), decision date, and canonical URL
(`https://www.dentalhearings.org/hearing/{hearingId}`).

## Usage

```bash
python bootstrap.py bootstrap          # Full pull (current published window)
python bootstrap.py bootstrap --sample # Fetch sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent hearings first)
python bootstrap.py test               # Quick connectivity test
```

## License

> ⚠️ **Commercial use restricted.** No explicit open licence is stated.

[Dental Professionals Hearings Service / GDC website terms](https://www.dentalhearings.org/terms-and-conditions)
— GDC fitness-to-practise and registration-appeal determinations are public
professional-regulator adjudication records published by the independent Dental
Professionals Hearings Service under the GDC's disclosure and publication policy.
The site states no explicit open licence (GDC / Crown copyright applies to the
underlying records), so commercial re-use is conservatively flagged, consistent
with the sibling UK professional-regulator tribunal sources (UK/GMC, UK/SDT,
UK/BTAS, UK/HCPTS, UK/NMC).
