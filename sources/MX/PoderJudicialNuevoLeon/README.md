# Poder Judicial del Estado de Nuevo Leon

**Source:** [https://www.pjenl.gob.mx/](https://www.pjenl.gob.mx/)
**Country:** MX
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** Web Application Firewall (WAF) blocks access

**Technical reason:** `imperva_incapsula_waf`

**Details:** pjenl.gob.mx behind Imperva Incapsula WAF — all non-browser requests return 403. CriteriosJudiciales JSON API exists but blocked. SentenciasPublicas is ASP.NET WebForms. ~257K decisions available if WAF bypassed.

## How you can help

The site's WAF blocks automated requests.
- Browser automation with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
