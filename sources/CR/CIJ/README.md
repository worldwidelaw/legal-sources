# Costa Rica CIJ (Judicial Info Center)

**Source:** [https://cij.poder-judicial.go.cr/](https://cij.poder-judicial.go.cr/)
**Country:** CR
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** Server blocks or errors

**Technical reason:** `server_blocks_automated_access`

**Details:** NexusPJ has 1.4M+ decisions with full text JSON API, but InfiSecure/FingerprintJS bot protection blocks all non-browser access. API returns 404 without JS-generated cookies. Needs Playwright. SCIJ (pgrweb.go.cr) has legislation but not court decisions.

## How you can help

The server blocks automated access or returns errors.
- If you know of an alternative endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
