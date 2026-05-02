# TJSP - Sao Paulo State Court Jurisprudence (E-SAJ)

**Source:** [https://esaj.tjsp.jus.br/cjsg/consultaCompleta.do](https://esaj.tjsp.jus.br/cjsg/consultaCompleta.do)
**Country:** BR
**Data types:** case_law
**Status:** Blocked

## Why this source is blocked

**Category:** CAPTCHA protection

**Technical reason:** `captcha_required`

**Details:** TJSP eSAJ CJSG search at esaj.tjsp.jus.br/cjsg/ now requires Google reCAPTCHA v3 (site key 6LcXJIAbAAAAAOwprTGEEYwRSe-HMYD-Ys0pSR6f) plus session UUID from captchaControleAcesso.do. Without valid token, POST to resultadoCompleta.do returns empty results section. Server-validated. DataJud API has metadata only (no decision text). Requires browser automation.

## How you can help

The site requires solving CAPTCHAs to access content.
- If you know of an alternative API or bulk download for this data, please let us know

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
