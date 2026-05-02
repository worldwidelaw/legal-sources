# ECHA Board of Appeal Decisions

**Source:** [https://echa.europa.eu/about-us/who-we-are/board-of-appeal/decisions](https://echa.europa.eu/about-us/who-we-are/board-of-appeal/decisions)
**Country:** EU
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** Web Application Firewall (WAF) blocks access

**Technical reason:** `azure_waf_protection`

**Details:** Azure WAF JS challenge blocks all HTTP requests (403). Both HTML pages and PDF documents are inaccessible programmatically. No API for BoA decisions exists.

## How you can help

The site's WAF blocks automated requests.
- Browser automation with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
