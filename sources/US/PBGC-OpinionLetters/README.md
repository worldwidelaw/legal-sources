# US/PBGC-OpinionLetters — PBGC Opinion Letters

Full text of the published **Opinion Letters** of the U.S. **Pension
Benefit Guaranty Corporation (PBGC)**, the federal agency that
administers Title IV of ERISA and insures private-sector defined-benefit
pension plans.

PBGC Opinion Letters are the agency's formal, published interpretive
determinations — e.g. whether a particular plan is covered under Title
IV, how the guarantee and premium provisions apply, and final decisions
on reconsideration requests. They are the PBGC counterpart to DOL and IRS
advisory opinions: authoritative agency interpretive guidance
(**doctrine**).

## Source

- **Database page:** https://www.pbgc.gov/employers-practitioners/legal-resources/opinion-letters/database
- Every letter is linked as `/documents/opinion-letter-{YY}-{NNN}`, and
  each of those URLs returns the letter's **PDF** directly.

## Build recipe

1. Fetch the database page and collect every `opinion-letter-YY-NNN` id.
2. Download each `/documents/{id}` PDF.
3. Extract full text with the shared `common.pdf_extract` extractor
   (older born-digital PDFs have a clean text layer; the rare scanned
   copy falls back to OCR).
4. `record_id` = the letter id (stable, unique). Letter number and date
   are parsed from the PDF body; the year is derived from the id as a
   fallback.

No auth, no CAPTCHA — builds locally.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain — U.S. federal government work (17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — PBGC Opinion Letters are works of a U.S. federal agency and are not subject to copyright. Free to use, including commercially.
