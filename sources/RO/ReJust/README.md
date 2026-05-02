# ReJust - National Jurisprudence Portal (Portal Național de Jurisprudență)

**Source:** [https://www.rejust.ro](https://www.rejust.ro)
**Country:** RO
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** CAPTCHA protection

**Technical reason:** `captcha_spa`

**Details:** Blazor WebAssembly SPA (.NET 8) protected by Cloudflare Turnstile CAPTCHA + ASP.NET antiforgery tokens. Internal API endpoints (Cases/QueryPublic, DosarEndpoint, etc.) discovered in WASM binary but require CAPTCHA solving and antiforgery tokens. Requires browser automation.

## How you can help

The site requires solving CAPTCHAs to access content.
- If you know of an alternative API or bulk download for this data, please let us know

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
