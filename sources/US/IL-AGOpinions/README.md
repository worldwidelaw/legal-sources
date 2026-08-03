# US/IL-AGOpinions — Illinois Attorney General Official Opinions

Full text of official opinions issued by the **Illinois Attorney General**.
Each opinion answers a legal question posed by a public official (State's
Attorney, state agency, legislator, local government) and constitutes an
authoritative interpretation of Illinois law (**doctrine**).

## Source

- Recent opinions: https://illinoisattorneygeneral.gov/opinions/
- Historical archive (1971–2009): https://illinoisattorneygeneral.gov/opinions/opinion-archive
- Master citation index: https://illinoisattorneygeneral.gov/Page-Attachments/Opinions%20Index%201971%20to%20Present%20-%20updated%20nov%2024%202025.pdf

Both listing pages are plain server-rendered HTML (no JavaScript, no CAPTCHA,
no pagination). Opinions are linked directly as PDFs in the site's `/dA/{hash}/`
document store, with the filename encoding `{YEAR} {NUMBER} {SUBJECT}` (e.g.
`2024 24-001 CRIMINAL LAW AND PROCEDURE  Authority of State's Attorney to
Disclose Brady Material Found in LEADS Reports.pdf`).

## Coverage

The scraper reads both listing pages and collects every `/dA/*.pdf` opinion
link (~1,469 total). The text-layer corpus that extracts cleanly **without OCR
is ~81 opinions**: **1992–1994** (~77, born-digital PDFs with real embedded
fonts) plus **2022–present** (4). The remaining ~1,388 opinions (1971–1991,
1995–2021) are **CCITT-fax scanned images** (zero text operators) and are
auto-skipped by the `<150`-char guard. An OCR-capable host (tesseract /
opendataloader) recovers the full ~1,469-opinion corpus with the same scraper.

## Fetching

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents (text-layer only)
python bootstrap.py bootstrap           # full pull (text-layer only without OCR)
```

The site WAFs the default `python-requests` UA, so pages and PDFs are fetched
via the `curl` CLI with a browser UA and passed to the extractor as bytes.
Requests are paced at ~1/second.

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Illinois Attorney General opinions are official US state government works in the
public domain. Commercial use permitted; no attribution required.
