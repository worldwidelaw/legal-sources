# US/OR-ERB — Oregon Employment Relations Board (ERB) Board Orders

Full text of the **Board Orders** of the **Oregon Employment Relations
Board (ERB)**, Oregon's independent quasi-judicial agency for public-sector
labor relations. ERB adjudicates unfair labor practice complaints,
representation and unit-clarification petitions, contract disputes,
declaratory rulings, and appeals under Oregon's **Public Employee
Collective Bargaining Act (PECBA, ORS 243)** and the State Personnel
Relations Law. Each Board Order resolves a specific contested case =
**case_law**.

## Source

- **Publisher:** Oregon Employment Relations Board / Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll9`, "Employment Relations Board Orders")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll9
- **Coverage:** ~4,859 Board Orders, 2004–present
- **Type:** `case_law`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth, no PDF extraction required:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll9/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (4,859) + `records[].dmrecord`
- **Per item**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll9/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → ERB Case Number (e.g. `UC-017-13 Board Order`)
  - `subjec` → Official Case Name
  - `date` → Date Issued (ISO `YYYY-MM-DD`)
  - `type` → Case Type
  - `transc` → **full-text Transcript** (born-digital / OCR text, the
    complete order body — tens of thousands of characters)

The `transc` field carries the full decision text, so no separate PDF
download or extraction is needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~4,859 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Board Orders of the Oregon Employment Relations Board are official quasi-judicial works of Oregon state government (government edicts) and are not subject to copyright. Free to use, including commercially. No attribution required.
