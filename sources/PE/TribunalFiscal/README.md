# Peru Tribunal Fiscal - Tax Court Resolutions

**Source:** [https://apps4.mineco.gob.pe/ServiciosTF/nuevo_busq_rtf.htm](https://apps4.mineco.gob.pe/ServiciosTF/nuevo_busq_rtf.htm)
**Country:** PE
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** Web Application Firewall (WAF) blocks access

**Technical reason:** `waf_protection`

**Details:** apps4.mineco.gob.pe and mef.gob.pe both behind Incapsula WAF (403). Direct PDF URLs return HTML error. Only 17 resolutions on gob.pe CDN (too few). Requires browser automation.

## How you can help

The site's WAF blocks automated requests.
- Browser automation with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
