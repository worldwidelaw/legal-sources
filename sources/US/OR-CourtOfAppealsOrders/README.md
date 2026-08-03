# US/OR-CourtOfAppealsOrders — Oregon Court of Appeals Orders of Interest

Full text of selected orders of the **Oregon Court of Appeals** ("Orders of
Interest") — orders resolving specific motions and matters in appeals before
the court, such as orders allowing or denying petitions, dismissals,
attorney-fee awards, and other case dispositions that do not issue as full
written opinions. Each order resolves a specific contested matter in a named
case = **case_law**.

## Source

- **Publisher:** Oregon Judicial Department / Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll17`, "Oregon Court of Appeals Orders of Interest")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll17
- **Coverage:** ~57 orders
- **Type:** `case_law`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth, no PDF extraction required:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll17/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (57) + `records[].dmrecord`
- **Per item**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll17/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → Case Title
  - `identi` → Case Number (e.g. `A186155`)
  - `subjec` → Case Classification
  - `descri` → Disposition (Allowed or Denied)
  - `creato` → Type of Order
  - `publis` → Issued by
  - `date` → Date of Order / Entry Date (ISO `YYYY-MM-DD`)
  - `transc` → **full-text Transcript** (born-digital / OCR text, the
    complete order body)

The `transc` field carries the full order text, so no separate PDF
download or extraction is needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~57 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Orders of the Oregon Court of Appeals are official judicial works of Oregon state government (edicts of government) and are not subject to copyright. Free to use, including commercially. No attribution required.
