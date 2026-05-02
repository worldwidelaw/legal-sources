# Austrian Federal Tax Court (BFG)

**Source:** [https://www.bfg.gv.at](https://www.bfg.gv.at)
**Country:** AT
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `spa_no_api`

**Details:** BFG decisions published via Findok (findok.bmf.gv.at) Angular SPA. NOT in RIS OGD API v2.6 - BFG not a valid Applikation. Findok API /api/neuInFindok/sync suchtypen=BFG returns guidelines (Richtlinien) not decisions. No public API for actual BFG Entscheidungen. Would require browser automation.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
