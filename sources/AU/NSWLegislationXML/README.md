# NSW Legislation Official XML Export

**Source:** [https://legislation.nsw.gov.au/](https://legislation.nsw.gov.au/)
**Country:** AU
**Data types:** legislation
**Status:** Blocked

## Why this source is blocked

**Category:** Cloudflare protection

**Technical reason:** `cloudflare_spa`

**Details:** Entire domain behind Cloudflare JS challenge (403 for all automated requests). SPA with no public API, no sitemap, no bulk download. XML available per-document but requires browser automation to bypass Cloudflare. ~1,100 Acts + 600 regulations + 300 EPIs.

## How you can help

The site uses Cloudflare anti-bot protection that blocks automated access.
- Browser automation (Playwright/Puppeteer) with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
