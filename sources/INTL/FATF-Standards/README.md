# FATF Anti-Money Laundering Standards

**Source:** [https://www.fatf-gafi.org/en/publications.html](https://www.fatf-gafi.org/en/publications.html)
**Country:** INTL
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** Cloudflare protection

**Technical reason:** `cloudflare_waf`

**Details:** fatf-gafi.org returns 403 on all automated requests (HTML, PDF, API). Cloudflare WAF with JS challenge. Requires browser automation (Playwright) not available in agent environment.

## How you can help

The site uses Cloudflare anti-bot protection that blocks automated access.
- Browser automation (Playwright/Puppeteer) with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
