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

`osteopathy.org.uk` was rebuilt on WordPress in 2026. The **"Decisions"** listing
moved to

```
https://www.osteopathy.org.uk/raising-a-concern/hearings/decisions/
```

and every published case (undertakings, interim suspension orders, council
decisions, PCC/HC hearing outcomes) is now a `hearing_decision` custom post type
served by the site's open **WP REST API**:

```
https://www.osteopathy.org.uk/wp-json/wp/v2/hearing_decision?per_page=100
```

Each item carries `id`, `slug`, `link`, `title` (which ends with the decision
date, e.g. *"Ms Poonam Shah – PCC Review Decision – 01 July 2026"*) and a
`hearing_decision_type` taxonomy term (undertaking / interim-suspension-order /
council-decision / professional-conduct-committee-and-health-committee-decisions).
Enumerating the API is preferred over scraping the listing tables: it is
paginated (`X-WP-TotalPages`), stable and returns the same set the page renders.
The rendered listing remains as a fallback enumeration path.

Each decision post lives at `/hearing-decision/{slug}/` and its body links the
reasoned decision as a **born-digital PDF** under `/wp-content/uploads/YYYY/MM/`
(content-type `application/pdf`, real text layer, no OCR): a structured header
(Case No / committee / hearing date / case-of name / committee members / legal
assessor) followed by the numbered reasoned decision.

The scraper pages the API, resolves each post's `.pdf` link inside `<main>` (so
site-wide policy PDFs are ignored), downloads the PDF and extracts the text layer
(PyMuPDF, with a shared pdfplumber/pypdf fallback). The published set is a
rolling window, so re-runs accumulate (the pipeline dedups on `_id` = the stable
decision slug).

**Yield:** 44 of the 46 currently published decisions extract full text
(643–303,945 chars, avg ~43K). The 2 skipped are image-only scans with no text
layer (`mr-alexander-taylor-ic-iso-decision-22-april-2024`, `undertaking`) and
would need OCR.

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
