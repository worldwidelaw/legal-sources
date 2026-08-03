# UK/HCPTS — Health and Care Professions Tribunal Service — Hearing Decisions

Full-text fitness-to-practise decisions of the **Health and Care Professions
Tribunal Service (HCPTS)**, the adjudication service of the **Health and Care
Professions Council (HCPC)** — the UK statutory regulator for 15 health and care
professions (paramedics, physiotherapists, dietitians, occupational therapists,
radiographers, chiropodists/podiatrists, biomedical scientists, practitioner
psychologists, speech & language therapists, and others).

HCPTS panels sit under the **Health and Social Work Professions Order 2001** and
the HCPC procedure rules. Their determinations — final hearings, interim order
applications/reviews, and substantive order reviews — set out the allegation, the
panel's reasoned findings of fact and impairment, and the order/sanction imposed
(caution, conditions of practice, suspension, striking off). This is binding
UK professional-regulator case law, not published on Find Case Law, and distinct
from the other regulator tribunals already covered (UK/GMC doctors, UK/SDT
solicitors, UK/BTAS barristers).

## Source

- **Publisher:** Health and Care Professions Tribunal Service (HCPTS) / HCPC
- **Site:** https://www.hcpts-uk.org/
- **Decisions:** https://www.hcpts-uk.org/hearings/search/
- **Coverage:** ~3,000+ full-text decisions (the live `sitemap.xml` catalogue)
- **Language:** English
- **Auth:** none (public)

## How it works

1. `GET /sitemap.xml` — a single XML document listing every
   `/hearings/hearings/{year}/{month}/{slug}/` decision page with a `<lastmod>`.
2. `GET` each decision page (clean server-rendered HTML). Skip pages that render
   the fixed "there has been a problem with the page" error (decisions not yet
   published or removed under the Publications Policy).
3. Extract the metadata block (Profession / Registration Number / Hearing Type /
   Date / Panel / Outcome) and the tabbed content divs (`#tab-allegation`,
   `#tab-finding`, `#tab-order`, `#tab-notes`); assemble the full determination
   text, dropping "No information currently available" placeholders. Born-digital
   HTML — no OCR.
4. One record per registrant hearing.

`fetch_updates(since)` re-scans the sitemap and re-fetches only pages whose
`<lastmod>` is on/after `since`.

## Record schema

`_id`, `_source` (`UK/HCPTS`), `_type` (`case_law`), `_fetched_at`, `title`,
`text` (full determination), `date` (hearing date, ISO 8601), `url`,
`registrant`, `profession`, `registration_number`, `hearing_type`, `panel`,
`outcome`, `court`, `jurisdiction` (`GB`), `language`.

## Usage

```bash
python bootstrap.py test              # connectivity + sample extraction
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full pull
python bootstrap.py update            # incremental (recently modified pages)
```

## License

> ⚠️ **Commercial use restricted.** No explicit open licence is stated on the
> HCPTS site; the underlying decisions are Crown-copyright public adjudication
> records published under the HCPC Publications Policy.

[HCPTS / HCPC website terms](https://www.hcpts-uk.org/terms-of-use/) —
attribution expected; commercial re-use conservatively flagged per project
policy, consistent with the sibling UK professional-regulator tribunal sources
(UK/GMC, UK/SDT, UK/BTAS).
