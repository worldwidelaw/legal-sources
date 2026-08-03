# US/LA-Register — Louisiana Register

Full text of the **Louisiana Register** — the official monthly publication of
the **Louisiana Office of the State Register** (La. R.S. 49:954.1), the state's
counterpart of the *Federal Register*. Each monthly issue publishes the state's
rulemaking record: gubernatorial **Executive Orders**, **Emergency Rules**,
adopted **Rules**, **Notices of Intent** (proposed rules), **Potpourri**,
committee reports and **Attorney General opinion summaries**. Adopted rules are
later codified into the Louisiana Administrative Code (`US/LA-AdministrativeCode`).
The rulemaking record is legally operative secondary legislation (`legislation`).

## Source

Published by the **State Library of Louisiana** as part of its CONTENTdm
["Louisiana Public Documents Digital Archive"](https://cdm16313.contentdm.oclc.org/digital/collection/p267101coll4)
(alias `p267101coll4`) on the OCLC-hosted CONTENTdm instance
`cdm16313.contentdm.oclc.org` — the same archive as `US/LA-AGOpinions`,
`US/LA-ExecutiveOrders` and `US/LA-AdministrativeCode`.

### Access recipe (public `dmwebservices` JSON API — no auth, no CAPTCHA)

1. **Enumerate** — a server-side title search isolates the Register issues:
   ```
   /digital/bl/dmwebservices/index.php?q=dmQuery/p267101coll4/title^Louisiana Register^all^and/dmrecord!title!date/date/{max}/{start}/1/0/0/0/json
   ```
   → `pager.total` = 640 monthly issues (1975–present); keep records whose
   title is exactly "Louisiana Register".
2. **Per item** — `dmGetItemInfo/p267101coll4/{dmrecord}/json` → Dublin-Core
   (title, date, descri = issue month, subjec).
3. **Full text** — `/digital/api/collection/p267101coll4/id/{dmrecord}/download`
   → PDF; born-digital text layer for the modern issues (extracted via the
   shared `common.pdf_extract` helper). The minority of pre-digital scanned
   issues (1970s–90s) yield 0 chars and are skipped by the <200-char guard.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 recent sample issues
python bootstrap.py bootstrap           # full pull (~640 issues)
```

Build locally with `/usr/bin/python3` (has `pdfplumber`/`fitz`).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — the Louisiana Register is an official work of Louisiana state government (edicts of government) and is not subject to copyright. Free to use, including commercially. No attribution required.
