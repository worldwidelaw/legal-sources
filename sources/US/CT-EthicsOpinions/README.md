# US/CT-EthicsOpinions — Connecticut Office of State Ethics: Advisory Opinions

Full text of the formal **Advisory Opinions** issued by the Connecticut Office of
State Ethics (OSE) and its predecessor, the State Ethics Commission, under the
Codes of Ethics for Public Officials and Lobbyists (Conn. Gen. Stat. § 1-79 *et
seq.*). An advisory opinion is the agency's written interpretation of the
conflict-of-interest, gift, revolving-door and disclosure statutes, requested by a
public official, state employee or lobbyist — official state legal interpretation
(**doctrine**).

## Source

- **Publisher:** Connecticut Office of State Ethics (OSE)
- **List page:** https://portal.ct.gov/ethics/advisory-opinions/numerical-list-and-summaries/advisory-opinions---summaries
- **Coverage:** 1993–present (~415 opinions)
- **Jurisdiction:** US-CT

## How it works

The OSE publishes a single "numerical list and summaries" page linking every
advisory opinion since 1993. Each opinion is an `<a href>` whose anchor text is
`Advisory Opinion No. YYYY-N`. The href is one of two shapes:

- **2012–present** → a born-digital PDF under
  `/-/media/ethics/advisory_opinions/{year}/...pdf` (real text layer; a handful of
  scanned PDFs fall back to OCR).
- **1993–2011** → an HTML detail page under
  `/ethics/advisory-opinions/{year}/advisory-opinion-{yyyy}-{n}`, whose opinion body
  lives in `<div class="small-12 medium-8 columns"><div class="content">`.

The scraper enumerates the list, downloads each document, extracts full text (PDF
text layer or HTML content column), and normalizes. Dead HTML links (CT.gov
soft-404 "Oops!" pages) are skipped. Dates are parsed from the first
`Month DD, YYYY` line near the top of the body, with a fallback to the
opinion-number year.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (1993–present)
```

## License

[Public Domain (State of Connecticut Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — advisory opinions of the Connecticut Office of State Ethics are official public records of the State of Connecticut, published for public use. No copyright restriction; commercial use permitted.
