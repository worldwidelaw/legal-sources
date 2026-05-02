# Supremo Tribunal Federal (Brazilian Supreme Court)

**Source:** [https://portal.stf.jus.br/](https://portal.stf.jus.br/)
**Country:** BR
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** Web Application Firewall (WAF) blocks access

**Technical reason:** `aws_waf_browser_required`

**Details:** All STF endpoints (portal, jurisprudencia, redir) behind AWS WAF requiring browser JS challenge. DATAJUD/CNJ has no STF index. digital.stf.jus.br has PDF download by known ID but no listing/search API. Needs Playwright/Selenium.

## How you can help

The site's WAF blocks automated requests.
- Browser automation with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
