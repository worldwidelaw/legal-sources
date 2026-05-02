# Luxembourg CSSF Enforcement Decisions

**Source:** [https://www.cssf.lu/en/enforcement/](https://www.cssf.lu/en/enforcement/)
**Country:** LU
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** Web Application Firewall (WAF) blocks access

**Technical reason:** `waf_blocks_requests`

**Details:** WAF/bot protection blocks all automated HTTP requests (curl, requests). Server closes connection without response. Requires browser automation (Playwright/Puppeteer). ~314 enforcement decisions + ~499 PDFs available behind WAF.

## How you can help

The site's WAF blocks automated requests.
- Browser automation with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
