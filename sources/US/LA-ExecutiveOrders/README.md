# US/LA-ExecutiveOrders — Louisiana Governor Executive Orders

Full text of the **Executive Orders of the Governor of Louisiana**. Under the
Governor's constitutional executive authority (La. Const. art. IV) and statute,
the Governor issues numbered Executive Orders that establish binding state
policy, create and direct executive agencies, allocate private-activity bond
volume, declare states of emergency, and otherwise regulate the executive
branch. Each Executive Order is a binding regulatory/administrative instrument =
**legislation** (which includes regulations).

## Source

Published by the **State Library of Louisiana** as part of its CONTENTdm
*Louisiana Public Documents Digital Archive* (collection alias `p267101coll4`)
on the OCLC-hosted CONTENTdm instance at `https://cdm16313.contentdm.oclc.org/`
— the same open `dmwebservices` JSON API used by the sibling
[`US/LA-AGOpinions`](../LA-AGOpinions/README.md).

## Access recipe

1. **Enumerate** — the archive holds ~44,700 heterogeneous public documents, so
   the Executive Orders are isolated with a server-side title search:

   ```
   GET /digital/bl/dmwebservices/index.php?q=dmQuery/p267101coll4/
       title^Executive Order^all^and/dmrecord!title!date/date/{max}/{start}/1/0/0/0/json
   ```

   → ~2,406 EOs, each numbered by governor (e.g. `BJ 15-25` Jindal, `KB 04-12`
   Blanco, `EW …` Edwin Edwards, `MJF 98-1` Mike Foster; pre-1980 EOs predate the
   governor-initial numbering).

2. **Per item** —

   ```
   GET /digital/bl/dmwebservices/index.php?q=dmGetItemInfo/p267101coll4/{dmrecord}/json
   ```

   returns the Dublin-Core metadata: `title`, `date` (ISO), `descri` (caption,
   e.g. *"BJ 15-25; Bond Allocation …; October 22, 2015"*), `creato` (issuer),
   `subjec` (subject headings). The EO number is parsed from the caption/body;
   when the DC `date` field is empty the spelled-out date in the caption is used.

3. **Full text** — the `transc` field is empty, so the order text is extracted
   from the item PDF:

   ```
   GET /digital/api/collection/p267101coll4/id/{dmrecord}/download
   ```

   The archived EO PDFs carry an embedded text layer; extraction uses the
   shared, OOM-hardened `common.pdf_extract` helper (pdfplumber → pypdf → OCR
   fallback). The rare pure image scans (0 chars) are skipped.

No CAPTCHA, no auth, reachable locally.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (~2,406 orders)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Louisiana Governor Executive Orders are official works of Louisiana state
government (edicts of government) and are not subject to copyright. Free to use,
including commercially.
