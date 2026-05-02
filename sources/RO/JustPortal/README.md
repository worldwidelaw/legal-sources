# Romanian Courts Portal (Portal Just)

**Source:** [https://portal.just.ro](https://portal.just.ro)
**Country:** RO
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** No full text access

**Technical reason:** `no_full_text_access`

**Details:** SOAP API at portalquery.just.ro/query.asmx only provides case metadata and short decision summaries (solutieSumar field). Full text judgments are in ROLII database, which is not publicly accessible via API. Alternative platforms (ReJust.ro, Lege5.ro) require browser automation or subscription.

## How you can help

The source only provides metadata (titles, dates) without full document text.
- If you know of a way to access full text for this source, please file an issue

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
