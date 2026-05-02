# Oregon Legislature OData API

**Source:** [https://api.oregonlegislature.gov/odata/odataservice.svc/](https://api.oregonlegislature.gov/odata/odataservice.svc/)
**Country:** US
**Data types:** legislation
**Status:** Blocked

## Why this source is blocked

**Category:** IP-based blocking

**Technical reason:** `ip_blocked`

**Details:** oregonlegislature.gov and api.oregonlegislature.gov timeout from this IP. OData API only covers bills, not ORS statutes. ORS available as HTML chapters but site unreachable.

## How you can help

The site blocks datacenter IP addresses.
- Works from residential IPs but not from cloud/VPS servers
- A residential proxy would solve this

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
