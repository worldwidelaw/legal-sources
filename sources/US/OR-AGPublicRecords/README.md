# US/OR-AGPublicRecords — Oregon Attorney General Public Records Orders

Full text of the **Public Records Orders** issued by the **Oregon Attorney
General**. Under Oregon's Public Records Law (**ORS 192.311–192.478**), a
person denied access to a public record held by a state agency may petition
the Attorney General, who issues a written **order** either directing the
agency to disclose the record or upholding the denial. Each order resolves a
specific petition/dispute = **case_law**.

This collection is **distinct** from `US/OR-AGOpinions` (the AG's formal
legal opinions).

## Source

- **Publisher:** Oregon Department of Justice / Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll2`, "Oregon Attorney General Public Records Orders")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll2
- **Coverage:** ~2,682 orders
- **Type:** `case_law`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth, no PDF extraction required:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll2/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (2,682) + `records[].dmrecord`
- **Per item**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll2/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → order caption (petitioner + order date)
  - `subjec` → public body / agency whose record was at issue
  - `date` → order date (ISO `YYYY-MM-DD`)
  - `transc` → **full-text Transcript** (the complete order body)

The `transc` field carries the full order text, so no separate PDF download
or extraction is needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~2,682 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Public Records Orders of the Oregon Attorney General are official works of Oregon state government (government edicts) and are not subject to copyright. Free to use, including commercially. No attribution required.
