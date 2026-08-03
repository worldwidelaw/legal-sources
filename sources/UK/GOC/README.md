# UK/GOC — General Optical Council — Fitness to Practise determinations

The **General Optical Council (GOC)** is the UK statutory regulator for
optometrists, dispensing opticians, student registrants and optical businesses.
Fitness to practise concerns are heard by the independent **GOC Fitness to
Practise Committee** under the Opticians Act 1989 and the General Optical
Council (Fitness to Practise) Rules 2013. Each concluded hearing publishes a
reasoned **determination** setting out the allegation, the facts found proved,
whether the registrant's fitness to practise is impaired, and the sanction /
order imposed (erasure, suspension, conditions, warning) or the interim order
made. These are binding professional-regulator adjudications = **case law**,
distinct from the sibling UK regulator sources: UK/GMC (doctors), UK/GDC
(dentists), UK/SDT (solicitors), UK/BTAS (barristers), UK/HCPTS (health & care
professions), UK/NMC (nurses/midwives), UK/GPhC (pharmacists) and
UK/SocialWorkEngland.

## Source

- **Publisher:** General Optical Council (GOC)
- **Data type:** `case_law`
- **Coverage:** the live published window of concluded GOC Fitness to Practise
  hearings (~80 determinations spanning ~2.5 years; ~12 months for warnings /
  suspension / conditions, up to 5 years for erasure per the GOC disclosure
  policy)
- **Language:** English
- **Auth:** none

## Access & structure

`optical.org` serves the **"Past hearings and outcomes"** page as a single
static HTML document:

```
https://optical.org/raising-concerns/hearings/past-hearings.html
```

It groups every published hearing under **year → month** collapsible panels,
each holding a `<table>` whose data rows are:

```
Hearing date | Registrant name | Outcome | Decision (link)
```

The month header (e.g. "July 2026") supplies the year for date cells that omit
it. Each Decision link points at a **born-digital PDF** served from
`https://optical.org/asset/{GUID}/` (content-type `application/pdf`, real text
layer, no OCR needed): a structured header (F(NN)NN case reference / registrant
name + registration number / hearing type / committee members / legal & clinical
advisers) followed by the reasoned determination.

The scraper walks the page in document order tracking the current month/year
heading, downloads each asset PDF, extracts the text layer (PyMuPDF, with a
shared pdfplumber/pypdf fallback) and yields full text. The page exposes a
rolling window, so re-runs accumulate (the pipeline dedups on `_id` = the stable
asset GUID).

## Usage

```bash
python bootstrap.py bootstrap          # Full pull (current published window)
python bootstrap.py bootstrap --sample # Fetch sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent hearings first)
python bootstrap.py test               # Quick connectivity test
```

## License

> ⚠️ **Commercial use restricted.** The GOC website states no explicit open
> licence; GOC / Crown copyright applies to the underlying records.

[GOC website terms](https://optical.org/terms-and-conditions.html) — GOC
fitness-to-practise determinations are public professional-regulator
adjudication records published under the GOC's disclosure and publication
policy. Attribution to the General Optical Council is expected. Commercial
re-use is conservatively flagged per project policy, consistent with the sibling
UK professional-regulator tribunal sources (UK/GMC, UK/GDC, UK/SDT, UK/BTAS,
UK/HCPTS, UK/NMC, UK/GPhC).
