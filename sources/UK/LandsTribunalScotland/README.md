# UK/LandsTribunalScotland — The Lands Tribunal for Scotland

Written Opinions and Notes of **The Lands Tribunal for Scotland**, the specialist
Scottish tribunal that determines disputes about land and property under the Lands
Tribunal Act 1949 and later statutes:

- **discharge/variation of title conditions** (real burdens);
- **disputed compensation** on compulsory purchase;
- **valuation for rating** appeals;
- **tenants' right-to-buy** references;
- **Land Register** appeals;
- **electronic-communications-code** (telecoms) references.

The Chairman of the Scottish Land Court also serves as President of the Lands
Tribunal; the two bodies share offices and staff but are separate judicial bodies.

These are adjudicative **case law** for the **Scotland (GB-SCT)** jurisdiction.
They are **not** covered by `UK/CaseLaw` (which indexes England & Wales superior
courts and reserved UK tribunals via the National Archives Find Case Law service).

## Source

- **Site:** http://www.lands-tribunal-scotland.org.uk/decisions/previous-decisions
- **Coverage:** ~460 decisions across eight subject categories
- **Citations:** neutral citation `[YYYY] LTS N` (recent decisions) and case
  reference `LTS/{CAT}/{YYYY}/{NNNN}`
- **Auth:** none (free public access)

## How it works

The `previous-decisions` index lists the subject categories
(`disputed-compensation`, `valuation-for-rating`, `tenants-rights-to-buy`,
`discharge-of-land-obligations`, `land-register-appeals`, `title-conditions`,
`ecc`, `others`). Each category page lists per-decision slugs
`/decisions/LTS.{CAT}.{YYYY}.{NN}`. Each decision page carries the full
Opinion/Note **inline as born-digital HTML** — no PDF, no OCR — with the citation,
case reference and tribunal members. The scraper strips the markup to recover the
full text and parses the metadata. One record per decision.

The decision date is taken from the labelled footer (`Decision issued:` /
`intimated to parties on`) or the modern header (`<date> Introduction`), falling
back to the neutral-citation year.

```
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent decisions)
python bootstrap.py test               # Connectivity/extraction test
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[SCTS website terms of use](https://www.scotcourts.gov.uk/terms-of-use) — the
Lands Tribunal for Scotland is administered alongside the Scottish Land Court
under the Scottish Courts and Tribunals Service (SCTS). SCTS terms permit
reproduction of judgments and decisions for personal and in-house use, but
restrict commercial re-use without consent. Same basis as `UK/ScotHousingChamber`
and `UK/ScotTaxChamber`. Not published under the Open Government Licence.
