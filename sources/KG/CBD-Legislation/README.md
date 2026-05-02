# Kyrgyzstan Central Database of Legal Information

**Source:** [http://cbd.minjust.gov.kg/](http://cbd.minjust.gov.kg/)
**Country:** KG
**Data types:** legislation
**Status:** Blocked

## Why this source is blocked

**Category:** Web Application Firewall (WAF) blocks access

**Technical reason:** `waf_ip_block`

**Details:** Has excellent OpenData API at /api/v1/OpenData/ with paginated search and full text HTML. 80,000-150,000+ documents. But WAF returns 403 Forbidden for all external requests. May be geo-restricted to CIS IPs. React SPA with reCAPTCHA.

## How you can help

The site's WAF blocks automated requests.
- Browser automation with stealth mode may work
- A residential proxy could bypass datacenter IP blocks

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
