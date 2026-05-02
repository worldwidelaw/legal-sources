# Bulgarian Supreme Administrative Court (ВАС)

**Source:** [https://info-adc.justice.bg/courts/portal/edis.nsf/e_acts.xsp](https://info-adc.justice.bg/courts/portal/edis.nsf/e_acts.xsp)
**Country:** BG
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** xsp_session_state

**Technical reason:** `xsp_session_state`

**Details:** BLOCKED: Old domain sac.government.bg is dead. New portal at info-adc.justice.bg uses IBM Lotus Domino XPages with server-side JSF session state. Search POST fails with 500 (session mismatch). Domino REST APIs return 403. Individual acts need id+guid pairs only discoverable via search. Requires headless browser automation.

## How you can help

- If you have access to this data source or know of an alternative, please file an issue
- Open a PR with suggestions or a working scraper

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
