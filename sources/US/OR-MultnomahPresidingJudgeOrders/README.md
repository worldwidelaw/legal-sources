# US/OR-MultnomahPresidingJudgeOrders — Multnomah County Circuit Court Presiding Judge Orders

Full text of the **Presiding Judge Orders** (PJO) of the **Multnomah
County Circuit Court**, Oregon's largest trial court. Under ORS 1.171 the
presiding judge administers the judicial district and issues standing
orders that establish **binding court policy and procedure** — traffic
and parking citation processing, animal-control citation handling,
records access and retention, departmental case assignment, financial /
fee matters, interpreter procedures, signature-stamp authority, and
related administrative rules. As the local administrative /
regulatory-rule instruments of the court they are classed as
**legislation** (which includes regulations).

## Source

- **Publisher:** Oregon Judicial Department / Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll15`, "Multnomah Presiding Judge Orders")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll15
- **Coverage:** ~753 orders, 1995–present
- **Type:** `legislation`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth, no PDF extraction required:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll15/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (753) + `records[].dmrecord`
- **Per item**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll15/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → Order Number (often the placeholder `No number`)
  - `subjec` → Subject (e.g. `Traffic`, `Records`, `Departmental Assignment`)
  - `descri` → Description
  - `publis` → County (Multnomah)
  - `date` → Date Signed (ISO `YYYY-MM-DD`)
  - `type` → Effective Date (ISO `YYYY-MM-DD`)
  - `format` → Expiration Date (ISO `YYYY-MM-DD`)
  - `transc` → **full-text Transcript** (born-digital / OCR text, the
    complete order body)

The `transc` field carries the full order text, so no separate PDF
download or extraction is needed.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~753 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Presiding Judge Orders of the Multnomah County Circuit Court are official works of Oregon state government (edicts of government) and are not subject to copyright. Free to use, including commercially. No attribution required.
