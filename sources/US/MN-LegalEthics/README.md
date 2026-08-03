# US/MN-LegalEthics — Minnesota Lawyers Professional Responsibility Board

Opinions promulgated by the **Minnesota Lawyers Professional Responsibility
Board (LPRB)** interpreting the **Minnesota Rules of Professional Conduct** to
advise lawyers. Cited as *LPRB Opinion No. N*.

- **Type:** doctrine (board opinions interpreting the Rules of Professional Conduct for lawyers)
- **Coverage:** ~26 born-digital opinions
- **Jurisdiction:** US-MN

## Source

- **Index:** https://lprb.mncourts.gov/lawyers-professional-responsibility-board-opinions/
  — a single public page listing every Board opinion as an anchor
  `Opinion N - [Month DD, YYYY -] Title` linking to a born-digital PDF under
  `/wp-content/uploads/`.
- **Documents:** born-digital PDFs (the upload-month path varies, so hrefs are
  taken verbatim), extracted with PyMuPDF (fitz), no OCR.

## Method

1. Fetch the index page; collect every opinion PDF anchor.
2. Parse the opinion number and (when present) date from the anchor text;
   de-duplicate on number.
3. Download each PDF, extract the full text; skip records under 200 chars.
4. Fall back to the first `Month DD, YYYY` in the body for the date.

## Distinct from

- **OLPR/Director "Advisory Opinions"** — informal ethics-hotline advice, not
  published one-per-file.
- **US/MN-CFBOpinions** — Campaign Finance & Public Disclosure Board (public
  officials).
- This source is the **lawyer** professional-conduct series (the MN member of
  the `US/{ST}-LegalEthics` vein).

## License

Public Domain — [17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
/ freely published board opinions — [LPRB Opinions](https://lprb.mncourts.gov/lawyers-professional-responsibility-board-opinions/).

LPRB Board Opinions are published free to the public on lprb.mncourts.gov (a
Minnesota Judicial Branch domain), with no login, paywall, or terms prohibiting
reuse. The LPRB is a body of the Minnesota Supreme Court established under the
Rules on Lawyers Professional Responsibility, and its opinions bind Minnesota
lawyers, so the government-edicts rationale applies directly. Commercial use OK.
