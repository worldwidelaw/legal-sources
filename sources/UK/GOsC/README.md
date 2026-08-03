# UK/GOsC — General Osteopathic Council — Fitness to Practise decisions

The **General Osteopathic Council (GOsC)** is the UK statutory regulator for
osteopaths under the **Osteopaths Act 1993**. Fitness to practise concerns are
heard by the independent **Professional Conduct Committee (PCC)** and **Health
Committee (HC)**, with interim orders made by the **Investigating Committee
(IC/ISO)**. Each concluded hearing publishes a reasoned **decision** setting out
the allegation, the facts found proved, whether the osteopath's fitness to
practise is impaired, and the sanction imposed (admonishment, conditions of
practice, suspension, removal from the Register) or the interim / undertaking
order made. These are binding professional-regulator adjudications = **case
law**, distinct from the sibling UK regulator sources: UK/GMC (doctors), UK/GDC
(dentists), UK/GOC (opticians), UK/GPhC (pharmacists), UK/SDT (solicitors),
UK/BTAS (barristers), UK/HCPTS (health & care professions), UK/NMC
(nurses/midwives) and UK/SocialWorkEngland.

## Source

- **Publisher:** General Osteopathic Council (GOsC)
- **Data type:** `case_law`
- **Coverage:** the live published window of concluded GOsC fitness-to-practise
  decisions (~40 decisions; older ones removed under the GOsC fitness-to-practise
  publication policy)
- **Language:** English
- **Auth:** none

## Access & structure

`osteopathy.org.uk` publishes a single **"Decisions"** listing page:

```
https://www.osteopathy.org.uk/raise-a-concern/hearings/decisions/
```

grouping published cases (undertakings, interim suspension orders, hearing
outcomes) as links to per-osteopath decision pages under
`/news-and-resources/document-library/fitness-to-practise/{slug}/`. The slug
encodes the osteopath name, committee (PCC / HC / IC-ISO) and the decision date.

Each decision page is a thin document-library wrapper that auto-downloads the
reasoned decision as a **born-digital PDF** linked with a relative `./{file}.pdf`
href (content-type `application/pdf`, real text layer, no OCR): a structured
header (Case No / committee / hearing date / case-of name / committee members /
legal assessor) followed by the numbered reasoned decision.

The scraper fetches the listing, collects every decision-page URL (a positive
committee-token allowlist filters out the policy / guidance pages), resolves each
page's `.pdf` link, downloads the PDF and extracts the text layer (PyMuPDF, with
a shared pdfplumber/pypdf fallback). The listing is a rolling window, so re-runs
accumulate (the pipeline dedups on `_id` = the stable decision slug).

## Usage

```bash
python bootstrap.py bootstrap          # Full pull (current published window)
python bootstrap.py bootstrap --sample # Fetch sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent decisions first)
python bootstrap.py test               # Quick connectivity test
```

## License

> ⚠️ **Commercial use restricted.** The GOsC website states no explicit open
> licence; GOsC / Crown copyright applies to the underlying records.

[GOsC website terms](https://www.osteopathy.org.uk/terms-and-conditions/) — GOsC
fitness-to-practise decisions are public professional-regulator adjudication
records published under the GOsC's fitness-to-practise publication policy.
Attribution to the General Osteopathic Council is expected. Commercial re-use is
conservatively flagged per project policy, consistent with the sibling UK
professional-regulator tribunal sources (UK/GMC, UK/GDC, UK/GOC, UK/SDT,
UK/BTAS, UK/HCPTS, UK/NMC, UK/GPhC).
