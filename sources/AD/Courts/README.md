# Andorra Tribunal Superior de Justícia

**Source:** [https://www.justicia.ad/cercador-jurisprudencia/](https://www.justicia.ad/cercador-jurisprudencia/)
**Country:** AD
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** JavaScript SPA (single-page application)

**Technical reason:** `spa_requires_browser_automation`

**Details:** Original URL tribunals.ad is dead. Actual site justicia.ad is a React SPA on Azure (csja-web.azurewebsites.net) with no public API and SSL cert issues. Requires Puppeteer/Playwright. Constitutional Court already covered by AD/TribunalConstitucional.

## How you can help

The site is a JavaScript single-page application with no API.
- Browser automation (Playwright) would be needed
- If you know of a hidden API endpoint, please share it

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
