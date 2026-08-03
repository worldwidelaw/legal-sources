# US/MA-EthicsOpinions — Massachusetts State Ethics Commission (EC-COI Advisory Opinions)

Full text of the formal conflict-of-interest advisory opinions ("EC-COI" opinions)
issued by the **Massachusetts State Ethics Commission** construing the
conflict-of-interest law (G.L. c. 268A) and the financial-disclosure law
(G.L. c. 268B). Each EC-COI opinion is the Commission's written interpretation of
those statutes at the request of a public official or employee — **doctrine**.

## Source

- Landing page: https://www.mass.gov/info-details/state-ethics-commission-public-legal-opinions
- Per-opinion pages: `https://www.mass.gov/opinion/ec-coi-{YY}-{N}` (born-digital HTML, 1979–early 2010s)

## Access & recipe

- **No auth, no CAPTCHA, no JavaScript.** There is no machine-readable index, so
  the corpus is enumerated by probing `ec-coi-{YY}-{N}` sequentially per year
  (N = 1, 2, 3, …) and stopping after a run of consecutive 404s.
- **User-Agent quirk:** `www.mass.gov` sits behind Akamai, which returns `403` for
  browser-like (Mozilla) UAs but serves a plain `python-requests` UA fine — the
  scraper sends that UA. (Same fingerprint quirk as `US/MA-DALA`.)
- **Full text:** the opinion body lives in `<div class="ma__rich-text">` blocks
  inside `<main id="main-content">`; these are joined and cleaned. No OCR needed.
- **Date:** parsed from the `Date: MM/DD/YYYY` header line, falling back to the
  opinion-number year.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (1979–present)
```

## License

[Public Domain — 17 U.S.C. § 105 / Commonwealth of Massachusetts government work](https://www.law.cornell.edu/uscode/text/17/105) — EC-COI advisory opinions are official public records of the Massachusetts State Ethics Commission, published for public use. Commercial use permitted; no attribution required.
