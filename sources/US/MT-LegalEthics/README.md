# US/MT-LegalEthics — State Bar of Montana, Ethics Committee

Ethics opinions issued by the **State Bar of Montana Ethics Committee**. Each
opinion interprets the **Montana Rules of Professional Conduct** as applied to
a member's contemplated conduct. Montana cites its opinions by a six-digit
code, e.g. *Ethics Opinion 970717*.

- **Type:** doctrine (advisory opinions interpreting the Rules of Professional Conduct for lawyers)
- **Coverage:** ~78 born-digital opinions, 1985–present
- **Jurisdiction:** US-MT

## Source

- **Index:** https://www.montanabar.org/For-Attorneys/State-Bar-Resources/Ethics-Opinions
  — a single public page listing every opinion PDF, grouped by Rule of
  Professional Conduct. The same opinion is often linked under several rules
  (and as `_N` filename variants), so records are de-duplicated on the base
  six-digit code.
- **Documents:** born-digital PDFs under `montanabar.org/Portals/MONTANA/`,
  extracted with PyMuPDF (fitz), no OCR.

## Method

1. Fetch the index page, collect every ethics-opinion PDF anchor.
2. Parse the six-digit citation code that leads the filename; de-duplicate on it.
3. Download each PDF and extract the full text; skip records under 200 chars.
4. Best-effort decode the date from the six-digit code (`YYMMDD`, or `MMDDYY`
   fallback).

## Distinct from

- **US/MT-Legislation**, **US/MT-Courts** — statutes and court decisions. This
  source is the **lawyer** professional-conduct advisory-opinion series (the MT
  member of the `US/{ST}-LegalEthics` vein).

## License

Public Domain / freely published advisory opinions — [Montana Ethics Opinions](https://www.montanabar.org/For-Attorneys/State-Bar-Resources/Ethics-Opinions).

State Bar of Montana ethics opinions are published free to the public on
montanabar.org, indexed on an open page, with no login, paywall, or terms
prohibiting reuse. The State Bar of Montana is an **integrated** (mandatory)
bar established by Montana Supreme Court rule, so the 17 U.S.C. § 105
government-edicts rationale applies fairly directly (like US/SC, US/LA, US/WI,
US/GA legal-ethics sources). Treated as effectively public domain — commercial
use OK.
