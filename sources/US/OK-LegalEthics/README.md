# US/OK-LegalEthics — Oklahoma Bar Association, Legal Ethics Opinions

Full text of the **advisory legal-ethics opinions** issued by the **Oklahoma
Bar Association's Legal Ethics Committee / Legal Ethics Advisory Panel**. Each
opinion interprets the **Oklahoma Rules of Professional Conduct** (and, for
older opinions, the predecessor Canons/Code) in response to a stated question,
to advise lawyers on their professional obligations = **doctrine** (advisory).

One continuous globally-numbered series, **Opinion No. 1 (1931) → No. 330+
(present)**; finalized opinions are published in the Oklahoma Bar Journal
(cited `___ OK LEG ETH ___`) and free as HTML on okbar.org. ~329 opinions.

- **Publisher:** Oklahoma Bar Association — Legal Ethics Committee
- **Listing page:** https://www.okbar.org/ethics/
- **Type:** doctrine
- **Jurisdiction:** US-OK

## How it works

1. The corpus is a paginated WordPress (Beaver Builder) archive at
   `/ethics/page/{p}/` (`p` = 1 … ~34, then HTTP 404). Each page links opinion
   detail pages as `/ethics/ethics-opinion-no-{N}/` (some carry a `-2` WP slug
   collision suffix).
2. The opinion **number** is parsed from the slug. When a number maps to more
   than one slug, the scraper fetches each candidate and keeps the one with the
   longest text.
3. Each detail page is clean **born-HTML**; the opinion body lives in the
   Beaver-Builder post-content module
   `div.fl-module-fl-post-content .fl-module-content` → extracted with
   BeautifulSoup, **no PDF, no OCR**.
4. The issue **date** is parsed from the `Adopted / Issued <Month DD, YYYY>`
   line into ISO 8601.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
```

## Distinct from

- **US/OK-Courts**, **US/OK-Legislation**, **US/OK-TaxDecisions** — other
  Oklahoma sources (appellate courts, statutes, tax tribunal decisions).

This is the 25th source in the state-bar attorney legal-ethics vein
(NC/AZ/TX/UT/VA/CA/WA/IL/NY/MI/OR/OH/ME/CT/VT/SC/WI/GA/KY/NH/AL/MO/TN/NV + OK).

## License

[Public Domain](https://www.law.cornell.edu/uscode/text/17/105) — U.S.
government-edict rationale (17 U.S.C. § 105). The Oklahoma Bar Association is
the state's **integrated (mandatory/unified) bar**, created by and operating
under the Supreme Court of Oklahoma; its ethics opinions are the work of a
government-authorized body regulating the legal profession, treated as public
domain consistent with the other state-bar legal-ethics sources. Published free
on okbar.org (and the Oklahoma Bar Journal) with no login, paywall or terms
prohibiting reuse. **Commercial use permitted.**
