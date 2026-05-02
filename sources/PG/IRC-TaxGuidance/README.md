# Papua New Guinea IRC Tax Rulings

**Source:** [https://www.irc.gov.pg](https://www.irc.gov.pg)
**Country:** PG
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `spa_requires_browser_automation`

**Details:** irc.gov.pg is a fully client-rendered Vue.js SPA. No API endpoints accessible. All HTTP requests return 1020-byte SPA shell. Requires browser automation.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
