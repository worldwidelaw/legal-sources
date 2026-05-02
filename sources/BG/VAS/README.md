# Bulgarian Supreme Administrative Court (VAS)

**Source:** [https://sac.justice.bg](https://sac.justice.bg)
**Country:** BG
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `javascript_spa`

**Details:** IBM Domino XPages SPA at info-adc.justice.bg requires JavaScript. Alternative legalacts.justice.bg has anti-CSRF protection blocking automation. Old URL sac.government.bg no longer resolves. New URL is sac.justice.bg. Case law database accessible only via JavaScript portal. See BLOCKED.md.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
