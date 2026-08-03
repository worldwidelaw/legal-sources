# UK/ScotHealthEducationChamber

**First-tier Tribunal for Scotland (Health and Education Chamber) — Additional Support Needs (ASN) decisions**

Anonymised full-text decisions of the Health and Education Chamber of the
First-tier Tribunal for Scotland, published by the Scottish Courts and Tribunals
Service (SCTS) at
[healthandeducationchamber.scot](https://healthandeducationchamber.scot/additional-support-needs/decisions).

The Chamber hears references and claims about the additional support needs of
children and young people (Education (Additional Support for Learning)
(Scotland) Act 2004), disability-discrimination claims against schools (Equality
Act 2010) and placing-request appeals. Its decisions are binding and appealable
to the Upper Tribunal for Scotland — adjudicative **case law for the GB-SCT
jurisdiction**, which is *not* covered by `UK/CaseLaw` (England & Wales +
reserved UK tribunals only) nor by the sibling Scottish chambers already in the
corpus (`UK/ScotHousingChamber`, `UK/ScotTaxChamber`, `UK/ScotLocalTaxChamber`,
`UK/LandsTribunalScotland`).

## Data

- **Type:** case_law
- **Jurisdiction:** GB-SCT (Scotland)
- **Coverage:** ~360 anonymised full-text decisions, ~2018–present (grows over time)
- **Language:** English
- **Auth:** none (free public access)

## How it works

1. Page the single Drupal "Views" listing
   `/additional-support-needs/decisions?page=N` (0-indexed, 10 rows/page). Each
   row yields the Chamber reference (e.g. `FTS/HEC/AR/24/0132`), decision date,
   category (e.g. *Placing Request*, *Co-ordinated Support Plan*, *Disability
   Claim*) and a link to the decision detail page.
2. Fetch each detail page and extract the **full decision text inline** from the
   `field--name-field-decision-text` container (born-digital HTML — no PDF
   download or OCR required).
3. A born-digital PDF of the same decision
   (`/sites/default/files/decisions/add/{ref}.pdf`) is used only as a fallback
   when the inline text field is missing or too short.

## Usage

```bash
python bootstrap.py test               # connectivity + parse check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (recent decisions)
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[SCTS terms of use](https://healthandeducationchamber.scot/terms-and-conditions)
— SCTS site terms permit reproduction "without formal permission or charge for
personal or in-house use only"; commercial re-use is restricted (flagged per
project policy). The underlying tribunal decisions are public records (Crown
copyright / SCTS) and are anonymised by the Chamber.
