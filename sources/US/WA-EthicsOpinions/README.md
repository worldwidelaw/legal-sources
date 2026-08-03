# US/WA-EthicsOpinions — Washington State Executive Ethics Board Advisory Opinions

Full text of the formal **Advisory Opinions** of the Washington State Executive
Ethics Board (EEB), issued under the Ethics in Public Service Act (**RCW 42.52**).

Each opinion is the Board's written, authoritative interpretation of the ethics
statutes it administers — conflicts of interest, use of state resources
(RCW 42.52.160), gifts, special privileges, honoraria, and post-employment
restrictions — applied to the facts presented in a request. State agencies rely
on them. This is official state legal interpretation → **doctrine**.

## Source

- **Publisher:** Washington State Executive Ethics Board
- **Listing:** https://ethics.wa.gov/advisories/advisory-opinions
- **Opinion PDFs:** `https://ethics.wa.gov/sites/default/files/public/AO%20{NN-NN}.pdf`
- **Coverage:** advisory opinions numbered `YY-NN`, 1996–present (~85 opinions)

## Access method

1. GET the single Drupal listing table (`/advisories/advisory-opinions`). Each
   row is one advisory opinion: the first cell holds the number (`YY-NN`) and the
   title link points to the opinion's born-digital PDF. The page is not truly
   paginated (`?page=1` returns the same table).
2. Download each PDF and extract its text layer with **PyMuPDF (fitz)** — the
   PDFs are born-digital, so no OCR is required.
3. Parse the approval date from the body (`APPROVAL DATE: Month [D,] YYYY`),
   falling back to the `YY` prefix of the opinion number.

No JavaScript, CAPTCHA, or authentication. Builds locally.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all advisory opinions)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Advisory Opinions of the Washington State Executive Ethics Board are official
state-government works in the public domain under the government-edicts doctrine.
No attribution required; commercial use permitted.
