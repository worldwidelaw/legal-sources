# Mexico CNDH Human Rights Recommendations

**Source:** [https://www.cndh.org.mx/tipo/1/recomendacion](https://www.cndh.org.mx/tipo/1/recomendacion)
**Country:** MX
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `spa_requires_browser_automation`

**Details:** Angular SPA with protected apiportal.cndh.org.mx API. All routes return same SPA shell. API endpoints refuse connections or return empty for non-browser requests. Requires headless browser.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
