# Peru SPIJ Open Data (National Open Data Platform)

**Source:** [https://www.datosabiertos.gob.pe/dataset/sistematizaci%C3%B3n-de-normas-legales-en-el-sistema-peruano-de-informaci%C3%B3n-jur%C3%ADdica-spij-desde](https://www.datosabiertos.gob.pe/dataset/sistematizaci%C3%B3n-de-normas-legales-en-el-sistema-peruano-de-informaci%C3%B3n-jur%C3%ADdica-spij-desde)
**Country:** PE
**Data types:** legislation, case_law
**Status:** Blocked

## Why this source is blocked

**Category:** Web Application Firewall (WAF) blocks access

**Technical reason:** `waf_blocked`

**Details:** Entire datosabiertos.gob.pe portal behind Huawei CloudWAF that blocks all automated requests (HTTP 418). Both CSV downloads and CKAN API return WAF block page. Dataset also appears metadata-only (no full text). Blocked 2026-03-24.

## How you can help

The site's WAF blocks automated requests.
- Browser automation with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
