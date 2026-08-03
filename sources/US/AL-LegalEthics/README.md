# US/AL-LegalEthics — Alabama State Bar Formal Ethics Opinions

Full text of the **Formal Ethics Opinions** issued by the **Alabama State Bar's
Disciplinary Commission / Office of General Counsel**. Each opinion is the Bar's
written interpretation of the **Alabama Rules of Professional Conduct** in
response to an inquiry about contemplated attorney conduct. The opinions are
advisory guidance for lawyers.

- **Publisher:** Alabama State Bar (the state's integrated/mandatory bar)
- **Coverage:** `RO-YYYY-NN` "Retained Opinion" series (with a few `FO-`/plain
  `YYYY-NN` variants), ~134 born-digital HTML opinions, **~1980s–present**
- **Type:** `doctrine` (advisory ethics opinions interpreting the Alabama RPC)
- **Full text:** yes — clean HTML from each opinion's detail page (no PDF/OCR)

## Source & method

- **Index:** opinions are enumerated from the public listing
  `/office-of-general-counsel/formal-opinions/`, which paginates
  `/office-of-general-counsel/formal-opinions/page/{N}/` (8 real pages; the
  loop stops on the first empty page). Each opinion is a
  `<div class="formal__opinion">` card exposing the number in
  `<div class="opinion__number"><strong>YYYY-NN</strong>`, an optional direct
  `/assets/.../Formal-Opinion-*.pdf` link, and a detail HTML link
  `/office-of-general-counsel/formal-opinions/{slug}/` (the slug is either the
  numeric `YYYY-NN` or a title slug, e.g. `fee-splitting`) carrying the title in
  `<span class="h3">`.
- **Detail pages:** the opinion body is rendered in clean HTML inside
  `<div class="content__container">` and extracted directly with BeautifulSoup.
  The direct PDF is used only as a fallback when the HTML body is thin.
- **Number:** canonical from the card / body header
  `ETHICS OPINION RO YYYY-NN` (tolerating O/0 OCR-style variants) or the PDF
  filename.
- **Date:** an explicit in-range `Month DD, YYYY` date in the body when present,
  else the opinion year → `YYYY-01-01`.

`alabar.org` returns HTTP 200 to a normal browser User-Agent (unlike several
sibling state-bar hosts such as `pabar.org` / `massbar.org`, which sit behind a
Cloudflare bot wall). No JavaScript, CAPTCHA or authentication is required.

## Distinct from

- **US/AL-AGOpinions** — Alabama Attorney General opinions.
- **US/AL-Courts** — Alabama court decisions.
- **US/AL-Legislation** — Alabama statutes.

This is the attorney professional-conduct advisory-opinion series that in other
states is built as `US/{ST}-LegalEthics`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## License

[Public Domain / freely published advisory opinions](https://www.alabar.org/office-of-general-counsel/formal-opinions/) — no attribution required.

Alabama State Bar Formal Opinions are published free to the public on alabar.org
as an educational service interpreting the Alabama Rules of Professional Conduct.
They carry no login, paywall or terms prohibiting reuse. Treated as effectively
public domain, consistent with the other state-bar legal-ethics sources. The
Alabama State Bar is the state's integrated (mandatory) bar, so the
17 U.S.C. § 105 government-edicts rationale applies directly. Commercial use
permitted.
