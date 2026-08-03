# US/OR-LUBA — Oregon Land Use Board of Appeals (LUBA) Final Orders & Opinions

Full text of the **Final Opinions and Orders** of the **Oregon Land Use
Board of Appeals (LUBA)**, Oregon's specialized quasi-judicial tribunal with
**exclusive jurisdiction** to review local government land-use decisions
(comprehensive plan and zoning amendments, conditional-use permits,
subdivisions, urban-growth-boundary and statewide-planning-goal compliance
disputes, etc.) under **ORS 197.805–197.860**. Each Final Opinion and Order
resolves a specific appeal = **case_law**.

## Source

- **Publisher:** Oregon Land Use Board of Appeals / Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll12`, "Oregon Land Use Board of Appeals Final Orders and
  Opinions")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll12
- **Coverage:** ~1,063 Final Orders & Opinions
- **Type:** `case_law`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth, no PDF extraction required:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll12/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (1,063) + `records[].dmrecord`
- **Per item**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll12/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → LUBA case number (e.g. `2023-071`)
  - `subjec` → Case name / parties (e.g. `DLCD v. Josephine County`)
  - `descri` → Document type (e.g. `Final Opinion and Order`)
  - `creato` → Authoring board member
  - `date` → Date issued (ISO `YYYY-MM-DD`)
  - `transc` → **full-text Transcript** (the complete order body)

The `transc` field carries the full order text, so no separate PDF download
or extraction is needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~1,063 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Final Opinions and Orders of the Oregon Land Use Board of Appeals are official quasi-judicial works of Oregon state government (government edicts) and are not subject to copyright. Free to use, including commercially. No attribution required.
