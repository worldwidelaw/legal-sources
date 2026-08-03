# US/SC-LegalEthics — South Carolina Bar Ethics Advisory Opinions

Full text of the **Ethics Advisory Opinions** issued by the **South Carolina
Bar's Ethics Advisory Committee**. Each opinion is the Committee's written
response, upon a member's request, on the ethical propriety of the inquirer's
contemplated conduct under the **South Carolina Rules of Professional Conduct**.
The opinions are advisory — the Committee has no disciplinary authority; lawyer
discipline is administered by the South Carolina Supreme Court through its
Commission on Lawyer Conduct.

- **Publisher:** South Carolina Bar (the state's integrated bar)
- **Coverage:** one numbered series `YY-NN` (with occasional letter-suffixed
  sub-opinions, e.g. `98-32c`, `98-32d`), ~506 opinions, **1989–present**
- **Type:** `doctrine` (advisory ethics opinions interpreting the RPC)
- **Full text:** yes — clean HTML from each opinion's detail page (no PDF/OCR)

## Source & method

- **Index:** `https://www.scbar.org/for-lawyers/quicklinks/legal-resources/ethics-advisory-opinions/`
  paginated via `?page=1..N` (10 opinions per page, ~51 pages). The scraper walks
  pages until two consecutive pages surface no new opinion slugs.
- **Detail pages:** `/…/ethics-advisory-opinion-{YY-NN}/` (newest omit
  "advisory": `/…/ethics-opinion-{YY-NN}/`). The opinion body is rendered in
  clean HTML inside the main `<article class="col-md-8 …">` column and extracted
  directly with BeautifulSoup; the "Download PDF Version" link and nav widgets
  are stripped.
- **Number:** parsed from the slug `YY-NN[letter]`, canonicalised to
  `YYYY-NN[letter]` (YY≥50 ⇒ 19YY, since the corpus runs 1989→2026).
- **Date:** SC EAOs are dated by year only (encoded in the number), so `date`
  defaults to `YYYY-01-01`; an explicit in-range date in the body is used when
  present.

No JavaScript, CAPTCHA or authentication is required.

## Distinct from

- **US/SC-JudicialEthics** — the S.C. Supreme Court's Advisory Committee on
  Standards of Judicial Conduct (advises *judges*), on sccourts.org.
- **US/SC-Courts** — court decisions.

This is the attorney professional-conduct advisory-opinion series that in other
states is built as `US/{ST}-LegalEthics`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## License

[Public Domain / freely published advisory opinions](https://www.scbar.org/for-lawyers/quicklinks/legal-resources/ethics-advisory-opinions/) — no attribution required.

South Carolina Bar Ethics Advisory Opinions are published free to the public on
scbar.org as an educational service interpreting the South Carolina Rules of
Professional Conduct. They are advisory and carry no login, paywall or terms
prohibiting reuse. Treated as effectively public domain, consistent with the
other state-bar legal-ethics sources. The South Carolina Bar is the state's
integrated (mandatory) bar, so the 17 U.S.C. § 105 government-edicts rationale
applies directly. Commercial use permitted.
