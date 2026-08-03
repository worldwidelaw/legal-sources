# US/OR-ExecutiveOrders — Oregon Governor Executive Orders

Full text of the **Executive Orders** issued by the **Governor of Oregon**.
Executive Orders are directives with the force of law issued under the
Governor's constitutional and statutory authority — declaring states of
emergency or drought, directing state agencies, establishing task forces and
boards, and setting executive policy. They are a form of subordinate
law-making = **legislation**.

## Source

- **Publisher:** Office of the Governor / Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll11`, "Oregon Governor Executive Orders")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll11
- **Coverage:** ~732 executive orders
- **Type:** `legislation`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth, no PDF extraction required:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll11/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (732) + `records[].dmrecord`
- **Per item**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll11/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → EO catalog number (e.g. `2020-57`)
  - `descri` → EO subject (e.g. `Extending the Duration of Executive Order No. 20-03`)
  - `date` → signing date (ISO `YYYY-MM-DD`)
  - `transc` → **full-text Transcript** (the complete order body). The
    canonical EO number (e.g. `20-57`) is also parsed from the
    `EXECUTIVE ORDER NO. …` heading in the text.

The `transc` field carries the full order text, so no separate PDF download
or extraction is needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~732 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Executive Orders of the Governor of Oregon are official edicts of Oregon state government (government edicts) and are not subject to copyright. Free to use, including commercially. No attribution required.
