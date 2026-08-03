# US/MO-LegalEthics — Missouri Ethics Opinions (Advisory Committee of the Supreme Court of Missouri)

Full text of the **Informal** and **Formal** ethics advisory opinions issued to
Missouri lawyers by the **Office of Legal Ethics Counsel** and the **Advisory
Committee of the Supreme Court of Missouri**, pursuant to **Missouri Supreme
Court Rule 5.30(c)**. Each opinion interprets the Missouri Rules of Professional
Conduct (Rule 4), the discipline rules (Rule 5) and the fees-to-practice rule
(Rule 6) in response to a lawyer's inquiry about contemplated conduct — advisory
guidance, not binding.

- **Publisher:** Advisory Committee of the Supreme Court of Missouri / Office of
  Legal Ethics Counsel (an arm of the Supreme Court of Missouri)
- **Coverage:**
  - **Informal Opinions** — `YYYY-NN` (modern) / `YYnnnn` (legacy, from
    **July 1, 1993**), ~1,046 published summaries.
  - **Formal Opinions** — a plain running integer (e.g. `115`), ~13 opinions.
- **Type:** `doctrine` (advisory ethics opinions interpreting the Missouri RPC)
- **Full text:** yes — clean HTML from each opinion page (no PDF/OCR)

## Source & method

- **Discovery:** opinion permalinks are enumerated exhaustively from the site's
  Yoast XML sitemaps — `/informalopinions-sitemap.xml` (+ `...-sitemap2.xml`)
  and `/formalopinions-sitemap.xml`. Informal permalinks are
  `/informal-opinion/{YYYY-NN|YYnnnn}/` and formal are `/formal-opinion/{N}/`.
  The handful of formal opinions are interleaved through the informal list so
  both series appear up front.
- **Detail pages:** each opinion page is a Divi layout in which every field is a
  `<div class="et_pb_text_inner">` block. The scraper reads the labelled
  `Opinion Number:`, `Adoption Date:`, `Rules:` and `Subject:` blocks for
  metadata, then takes the longest remaining non-boilerplate block as the body
  (Question/Answer for informal opinions, the numbered `FORMAL OPINION` text for
  formal ones). A screen-reader artefact (` dash` inside `<span class="sr-only">`)
  is stripped so cited rules read `4-1.15` rather than `4 dash-1.15`.
- **Number:** the printed opinion number. Legacy informal numbers `YYnnnn` carry
  the 2-digit year (`93` → 1993); formal numbers are plain integers.
- **Date:** the explicit `Adoption Date` when present, else an in-body
  month-date, else the number's year → `YYYY-01-01`.

No JavaScript, CAPTCHA or authentication is required.

## Distinct from

- **US/MO-Courts** — Missouri court decisions.
- **US/MO-Legislation** — Missouri statutes.

This is the attorney professional-conduct advisory-opinion series that in other
states is built as `US/{ST}-LegalEthics`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records (both series)
python bootstrap.py bootstrap            # full pull (all opinions)
```

## License

[Public Domain / freely published advisory opinions](https://mo-legal-ethics.org/informal-opinions-search/) — no attribution required.

Missouri informal and formal ethics opinions are published free to the public on
mo-legal-ethics.org by the Office of Legal Ethics Counsel and the Advisory
Committee of the Supreme Court of Missouri as an educational service interpreting
the Missouri Rules of Professional Conduct. They carry no login, paywall or terms
prohibiting reuse. The Advisory Committee is an arm of the Supreme Court of
Missouri, so the 17 U.S.C. § 105 government-edicts rationale applies directly.
Treated as public domain, consistent with the other state-bar legal-ethics
sources. Commercial use permitted.
