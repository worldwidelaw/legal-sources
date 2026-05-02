# ROLII (Romanian Legal Information Institute)

**Source:** [https://www.rolii.ro/](https://www.rolii.ro/)
**Country:** RO
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `spa_no_api`

**Details:** SPA (YMS framework) with authenticated API. CSM (Superior Council of Magistracy) ordered ROLII to disable all programmatic/third-party API access in March 2022. The YMS API at /yms/api/v1/ returns 401. No public REST/JSON/SPARQL endpoints. REJUST (alternative) uses Blazor WASM + Cloudflare Turnstile CAPTCHA. portal.just.ro SOAP provides metadata only (no full text). Requires browser automation.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
