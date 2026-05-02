# Lithuanian Data Protection Authority (VDAI)

**Source:** [https://vdai.lrv.lt](https://vdai.lrv.lt)
**Country:** LT
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** Cloudflare protection

**Technical reason:** `cloudflare_protection`

**Details:** BLOCKED: Entire vdai.lrv.lt domain behind Cloudflare JS challenge (403 on all requests). No API, no open data for decisions. PDFs exist at /public/canonical/ URLs but inaccessible without browser automation. data.gov.lt has only statistics, not decision texts.

## How you can help

The site uses Cloudflare anti-bot protection that blocks automated access.
- Browser automation (Playwright/Puppeteer) with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
