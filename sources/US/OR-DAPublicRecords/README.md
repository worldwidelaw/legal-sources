# US/OR-DAPublicRecords — Oregon District Attorney Public Records Orders

Full text of the **public-records appeal orders** issued by **Oregon
District Attorneys**. Under the Oregon Public Records Law (ORS 192.415), a
person whose request for records held by a **local** public body is denied
may petition the District Attorney of the county in which the body is
located; the DA adjudicates the petition and issues a written order
granting or denying disclosure — the county-level counterpart of the
Attorney General's public-records orders for state agencies
(`US/OR-AGPublicRecords`). Each order resolves a specific contested
public-records appeal = **case_law**.

## Source

- **Publisher:** Oregon State Library
- **Platform:** OCLC-hosted **CONTENTdm** digital collection
  (alias `p17027coll4`, "District Attorney Public Records Orders")
- **Landing page:** https://cdm17027.contentdm.oclc.org/digital/collection/p17027coll4
- **Coverage:** ~89 orders
- **Type:** `case_law`

## Access method

CONTENTdm exposes a public, un-authenticated JSON web-services API
(`dmwebservices`). No CAPTCHA, no auth:

- **Enumerate pointers**
  `…/dmwebservices/index.php?q=dmQuery/p17027coll4/0/dmrecord!date/dmrecord/{max}/{start}/0/0/0/0/json`
  → `pager.total` (89) + `records[].dmrecord`
- **Per item metadata**
  `…/dmwebservices/index.php?q=dmGetItemInfo/p17027coll4/{dmrecord}/json`
  → Dublin-Core fields:
  - `title` → Case Name
  - `subjec` → Petitioner
  - `descri` → Respondent (public body)
  - `county` → County
  - `type` → Primary Exemptions at Issue
  - `format` → Records Requested
  - `source` → Result
  - `date` → Date of Order (ISO `YYYY-MM-DD`)

Unlike the sibling collections, the `transc` (Transcript) field is **empty**
here, so the full order text is extracted from each item's **PDF**,
downloaded from the CONTENTdm download endpoint
`…/digital/api/collection/p17027coll4/id/{dmrecord}/download`. PDF text
extraction uses the shared, OOM-hardened `common.pdf_extract` helper.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~89 orders)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — District Attorney public-records orders are official quasi-judicial works of Oregon state/local government (edicts of government) and are not subject to copyright. Free to use, including commercially. No attribution required.
