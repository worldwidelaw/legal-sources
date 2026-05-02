# India Supreme Court Judgments (SCI API)

**Source:** [https://api.sci.gov.in](https://api.sci.gov.in)
**Country:** IN
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `no_api_access`

**Details:** 2026-04-01 api.sci.gov.in returns empty responses (Content-Length 0) on all endpoints. SCR search requires CAPTCHA. verdictfinder times out. Already covered by IN/SCJudgments (35K+ judgments via AWS Open Data).

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
