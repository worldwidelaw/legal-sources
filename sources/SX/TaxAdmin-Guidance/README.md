# Sint Maarten Tax Administration Guidance

**Source:** [https://www.sintmaartengov.org/government/tax](https://www.sintmaartengov.org/government/tax)
**Country:** SX
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `sharepoint_spa_requires_browser_automation`

**Details:** sintmaartengov.org is SharePoint-based with dynamically loaded content. Laws, tax forms, and gazette pages return empty HTML — content requires JS rendering. No static PDFs or API endpoints accessible.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
