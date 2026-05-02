# Danish Parliament Open Data (Folketingets Åbne Data)

**Source:** [https://oda.ft.dk](https://oda.ft.dk)
**Country:** DK
**Data types:** parliamentary_proceedings
**Status:** Blocked

## Why this source is blocked

**Category:** Cloudflare protection

**Technical reason:** `cloudflare_no_full_text`

**Details:** BLOCKED: OData API at oda.ft.dk works but only provides metadata + brief resume summaries (~1000 chars). Full text PDFs hosted on www.ft.dk are blocked by Cloudflare (403 'Just a moment' challenge). FTP also refuses connections. No alternative full text source available.

## How you can help

The site uses Cloudflare anti-bot protection that blocks automated access.
- Browser automation (Playwright/Puppeteer) with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
