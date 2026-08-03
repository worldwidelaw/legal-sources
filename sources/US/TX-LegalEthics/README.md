# US/TX-LegalEthics — Texas Committee on Professional Ethics (Ethics Opinions)

Full text of the ethics opinions issued by the **Professional Ethics Committee
for the State Bar of Texas**. The Committee — nine members appointed by the
Supreme Court of Texas — expresses its view on the propriety of professional
conduct under the **Texas Disciplinary Rules of Professional Conduct**, either
on its own initiative or in response to a request from a member of the bar.

Each opinion answers a specific inquiry and states the Committee's conclusion =
**doctrine** (the Committee's official written interpretation of the attorney-
conduct rules).

- **Publisher / repository:** [Texas Center for Legal Ethics](https://www.legalethicstexas.com/resources/opinions/) (the official repository)
- **Coverage:** one continuous numbered series, Opinion 1 (1966) to present
  (Opinion 710 as of 2026-07)
- **Format:** born-digital HTML (no OCR, no PDF, no CAPTCHA, no auth)
- **Type:** doctrine

## How it works

Each opinion is a static page at a predictable sequential URL:

```
https://www.legalethicstexas.com/resources/opinions/opinion-{N}/
```

The scraper reads the current highest opinion number from the newest-first
index page (`/resources/opinions/`), then walks `opinion-1 .. opinion-{max}`
(with a small look-ahead buffer), skipping any 404 (withdrawn) number. The full
text lives in `<div class="resourcesDetail">` — the *Question Presented* block
plus the *Statement of Facts*, *Discussion* and *Conclusion* tab panels are all
present in the HTML and are de-tagged after removing the search widget, tab
buttons and social links. The date comes from the page's `<time>` element when
present (modern opinions), otherwise from the Bluebook citation year.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## Distinct from

- **Texas Ethics Commission** — advises *public officials*, not lawyers.
- **Texas Attorney General opinions** — legal opinions of the state AG.

This source covers *attorney* professional-responsibility opinions, part of the
state-bar legal/attorney-ethics vein (parallel to `US/NC-LegalEthics`).

## License

[Public Domain — U.S. Government / State Regulatory-Agency Official Record](https://www.law.cornell.edu/uscode/text/17/105) — ethics opinions of the Professional Ethics Committee for the State Bar of Texas are official public records of a Texas regulatory body (the Committee is appointed by the Supreme Court of Texas), published for public use by the Texas Center for Legal Ethics with no copyright restriction. Commercial use permitted; no attribution required.
