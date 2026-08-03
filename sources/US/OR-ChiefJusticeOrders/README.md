# US/OR-ChiefJusticeOrders — Oregon Chief Justice Orders & Supreme Court Orders

Full text of the administrative orders of the **Oregon Supreme Court /
Chief Justice** — **Chief Justice Orders** (CJO) and **Supreme Court
Orders** (SCO). As the administrative head of the Oregon Judicial
Department (ORS 1.002), the Chief Justice issues these orders to **adopt
and amend binding court rules** — the Uniform Trial Court Rules, Oregon
State Bar and Board of Bar Examiners rules, court fee, register and
procedure rules — and to make administrative directives such as
departmental appointments and pro tem / presiding-judge designations.
Because they are the regulatory / administrative-rule instruments of the
Oregon judiciary, they are classed as **legislation** (which includes
regulations).

## Source

- **Publisher:** Oregon Judicial Department / Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll10`, "Oregon Chief Justice Orders")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll10
- **Coverage:** ~2,610 orders, 1985–present
- **Type:** `legislation`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth, no PDF extraction required:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll10/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (2,610) + `records[].dmrecord`
- **Per item**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll10/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → Order Number (e.g. `2013-005`)
  - `subjec` → Subject keyword string (e.g. `Rules, Uniform Trial Court Rules`)
  - `descri` → Description (short caption of the order)
  - `type` → Type (`CJO` = Chief Justice Order, `SCO` = Supreme Court Order)
  - `date` → Date Signed (ISO `YYYY-MM-DD`)
  - `langua` → Effective Date (ISO `YYYY-MM-DD`)
  - `transc` → **full-text Transcript** (born-digital / OCR text, the
    complete order body)

The `transc` field carries the full order text, so no separate PDF
download or extraction is needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~2,610 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Chief Justice Orders and Supreme Court Orders of the Oregon judiciary are official works of Oregon state government (edicts of government) and are not subject to copyright. Free to use, including commercially. No attribution required.
