# US/CopyrightReviewBoard — U.S. Copyright Office Review Board Decisions

Final decisions of the **Review Board of the United States Copyright Office**
(Library of Congress). When the Copyright Office's Registration Program refuses to
register a work and the applicant files a *second request for reconsideration*, the
Review Board — the Office's highest-level adjudicative body — issues a written
decision that either **affirms** or **reverses** the refusal (37 C.F.R. § 202.5).
Each decision applies the Copyright Act's originality / copyrightability standards
to a specific work and is the Office's final agency action on that application.

- **Type:** `case_law` (adjudicative decision on a specific matter)
- **Jurisdiction:** US (federal)
- **Corpus:** ~745 decisions, born-digital full-text PDFs (no OCR needed)
- **Auth:** none — no login, no CAPTCHA, no JavaScript

## Access

Every decision is listed in a single HTML table at
<https://www.copyright.gov/rulings-filings/review-board/>. Each row links to the
decision PDF at `/rulings-filings/review-board/docs/{slug}.pdf` and carries the
year, work categories and outcome. The scraper parses the table, downloads each
born-digital PDF, extracts its text layer via the shared `common.pdf_extract`
backend, and parses the precise decision date from the "Month DD, YYYY" head of the
letter (falling back to the listing table's year).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all decisions)
```

## License

[Public Domain (US Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the U.S. Copyright Office Review Board are works of the U.S. federal government and are in the public domain. Commercial use permitted; no attribution required.
