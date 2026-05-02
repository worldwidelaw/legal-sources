# European Chemicals Agency (ECHA)

**Source:** [https://echa.europa.eu](https://echa.europa.eu)
**Country:** EU
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** Cloudflare protection

**Technical reason:** `cloudflare_or_waf`

**Details:** ECHA website returns 403 Forbidden to all automated requests. ECHA CHEM requires login. EU Open Data Portal datasets only link to HTML pages on echa.europa.eu. Board of Appeal decisions require JavaScript rendering. No accessible API or bulk download available.

## How you can help

The site uses Cloudflare anti-bot protection that blocks automated access.
- Browser automation (Playwright/Puppeteer) with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
