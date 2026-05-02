# Saudi Arabia Courts Decisions Portal

**Source:** [https://laws.moj.gov.sa/](https://laws.moj.gov.sa/)
**Country:** SA
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `spa_requires_browser_automation`

**Details:** Nuxt.js SPA on laws.moj.gov.sa. Backend API at laws-gateway.moj.gov.sa is behind Apigee with JWT auth (Saudi IAM/Absher). No public REST API available.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
