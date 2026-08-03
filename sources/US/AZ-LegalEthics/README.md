# US/AZ-LegalEthics — State Bar of Arizona: Legal (Attorney) Ethics Opinions

Formal Ethics Opinions issued by the **State Bar of Arizona's** Committee on the
Rules of Professional Conduct (1985–2016), together with the most recent opinions
of the **Arizona Supreme Court Attorney Ethics Advisory Committee** (indexed in
the same database). Each opinion interprets the **Arizona Rules of Professional
Conduct** (Ethical Rules, "ER …") and answers a specific inquiry about a lawyer's
professional-responsibility obligations. This is the official written
interpretation of the attorney-conduct rules = **doctrine**.

~150+ opinions, 1985–present.

## Access

Public JSON REST API at `https://api.azbar.org/` — no CAPTCHA, no per-user auth.

Every request carries a **static, public** credential trio that the ethics-opinions
page ships in plain JavaScript to every visitor (this is a fixed public key, not a
login): `userid: publictools`, a fixed `password` GUID, `updatedBy: Hub`. The
scraper reads the trio live from the listing page (with a hard-coded fallback).

1. Enumerate opinions by year:
   ```
   GET /EthicsRules/OpinionSearch/ByYear/?Year={YYYY}
   -> {"Result": [{Id, OpinionNumber, Title, OpinionDate, Summary}, …]}
   ```
2. Fetch full text per opinion:
   ```
   GET /EthicsRules/Opinion/?Id={id}
   -> {"Result": {Body (full HTML), Title, OpinionNumber, OpinionDate, Summary, Note}}
   ```
   `Body` is de-tagged to clean full text (Facts, analysis, ER citations,
   footnotes); `Note`, if present, is appended.

## Vein

Second source in the **state-bar attorney legal-ethics** doctrine vein after
`US/NC-LegalEthics`. Distinct from judicial-ethics opinions (which advise judges)
and from executive state-ethics commissions (which advise public officials).

## Usage

```bash
python bootstrap.py test-api              # connectivity + extraction test
python bootstrap.py bootstrap --sample    # ~12 sample records
python bootstrap.py bootstrap             # full pull
```

## License

[Public Domain (Arizona Regulatory-Agency Official Record)](https://www.law.cornell.edu/uscode/text/17/105) — formal ethics opinions of the State Bar of Arizona / Arizona Supreme Court committees are official public records published for public use with no copyright restriction. Commercial use permitted. (The opinions themselves note they are advisory and non-binding.)
