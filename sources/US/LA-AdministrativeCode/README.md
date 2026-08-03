# US/LA-AdministrativeCode — Louisiana Administrative Code (LAC)

Full text of the **Louisiana Administrative Code (LAC)** — the official
codification of the rules and regulations adopted by Louisiana state agencies
under the state Administrative Procedure Act (La. R.S. 49:950 *et seq.*). The
LAC is organized into ~50 numbered **Titles** (e.g. Title 37 Insurance, Title
61 Revenue and Taxation, Title 76 Wildlife and Fisheries), each divided into
Parts. Agency regulations are legally binding secondary legislation
(`legislation`).

## Source

Published by the **State Library of Louisiana** as part of its CONTENTdm
["Louisiana Public Documents Digital Archive"](https://cdm16313.contentdm.oclc.org/digital/collection/p267101coll4)
(alias `p267101coll4`) on the OCLC-hosted CONTENTdm instance
`cdm16313.contentdm.oclc.org`. Sibling of `US/LA-AGOpinions` and
`US/LA-ExecutiveOrders`, which draw from the same archive.

### Access recipe (public `dmwebservices` JSON API — no auth, no CAPTCHA)

1. **Enumerate** — a server-side title search isolates the LAC volumes from the
   ~44,700-item archive:
   ```
   /digital/bl/dmwebservices/index.php?q=dmQuery/p267101coll4/title^Louisiana Administrative Code^all^and/dmrecord!title!date/title/{max}/{start}/1/0/0/0/json
   ```
   → `pager.total` = 736 volumes; keep records whose title starts with
   "Louisiana Administrative Code".
2. **Per item** — `dmGetItemInfo/p267101coll4/{dmrecord}/json` → Dublin-Core
   fields (title, date `YYYY-MM`/`YYYY`, descri = edition, subjec = subjects).
3. **Full text** — `/digital/api/collection/p267101coll4/id/{dmrecord}/download`
   → born-digital PDF (embedded text layer); extracted via the shared
   `common.pdf_extract` helper. Each LAC volume is a full Title/Part (hundreds
   of pages → millions of chars of regulatory text); volumes are streamed one
   at a time.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample volumes
python bootstrap.py bootstrap           # full pull (~736 volumes)
```

Build locally with `/usr/bin/python3` (has `pdfplumber`/`fitz`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — the Louisiana Administrative Code is an official work of Louisiana state government (edicts of government) and is not subject to copyright. Free to use, including commercially. No attribution required.
