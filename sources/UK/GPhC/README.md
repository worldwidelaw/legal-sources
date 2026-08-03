# UK/GPhC — General Pharmaceutical Council — Fitness to Practise determinations

The **General Pharmaceutical Council (GPhC)** is the UK statutory regulator for
~60,000 pharmacists and pharmacy technicians and ~14,000 registered pharmacies in
Great Britain. Its **Fitness to Practise Committee** sits under the **Pharmacy
Order 2010**. Concluded hearings publish a reasoned **"determination"** setting
out the allegation/charges, the facts found proved, whether the registrant's
fitness to practise is impaired, and the sanction/order imposed (removal,
suspension, conditions, warning) or the interim order made.

These are binding professional-regulator adjudications = **case law**, distinct
from UK/GMC (doctors), UK/SDT (solicitors), UK/BTAS (barristers), UK/HCPTS
(health & care professions), UK/NMC (nurses/midwives), UK/GDC (dentists) and
UK/SocialWorkEngland.

## Access & structure (all public, no auth)

- The GPhC **Hearings** page lists forthcoming and past hearings in a Drupal
  Views table. Each concluded past-hearing row carries the hearing date,
  registrant name, category (pharmacist / pharmacy technician), registration
  number, type of hearing and outcome, plus a **"Read determination"** link to a
  born-digital PDF at
  `https://files.pharmacyregulation.org/determinations/D{NNNNNN}/D{NNNNNN}.pdf`.
- The determination PDFs have a real text layer (**no OCR**): a structured header
  (committee / hearing type / dates / registrant name / registration number /
  part of the register / type of case / committee members / representation)
  followed by the numbered reasoned determination.
- GPhC publishes determinations only where fitness to practise is impaired and
  keeps each on the website for **~12 months**, so one run captures the current
  rolling window (~240 determinations) and re-runs accumulate the record (the
  pipeline dedups on `_id` = the stable determination number `D{NNNNNN}`).
- `www.pharmacyregulation.org` UA-gates plain clients (HTTP 403) but serves a
  browser User-Agent (HTTP 200); the whole past-hearings window renders on the
  single Hearings page (no pagination).

## Output

Normalized `case_law` records with full determination text plus metadata:
registrant name, registration number, profession, hearing type, outcome,
determination number, decision date, and the determination PDF URL.

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

[GPhC website terms and conditions](https://www.pharmacyregulation.org/about-us/website-information/terms-and-conditions)
— GPhC fitness-to-practise determinations are public professional-regulator
adjudication records published under the GPhC's publication policy (kept online
~12 months). The site states no explicit open licence (GPhC / Crown copyright
applies to the underlying records), so commercial re-use is conservatively
flagged, consistent with the sibling UK professional-regulator tribunal sources
(UK/GMC, UK/SDT, UK/BTAS, UK/HCPTS, UK/NMC, UK/GDC).
