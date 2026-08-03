# US/HI-HLRB — Hawaii Labor Relations Board (Decisions and Orders)

Full text of the **decisions and orders of the Hawaii Labor Relations
Board (HLRB)**, the state's quasi-judicial agency that administers
Hawaii's public-sector collective-bargaining law (HRS chapter 89) and
the Hawaii Employment Relations Act (HRS chapter 377). The Board
adjudicates prohibited-practice complaints, bargaining-unit /
representation and clarification petitions, declaratory rulings, impasse
and other contested cases. Each numbered Decision (No. 1 in 1971 to the
present) and Order resolves a specific contested case, so the records are
**case_law**.

## Source

- Index page: <https://labor.hawaii.gov/hlrb/decisions/>
- Decision PDFs: `https://labor.hawaii.gov/hlrb/files/{YYYY}/{MM}/Decision-No.-{N}.pdf`
- Order PDFs: `https://labor.hawaii.gov/hlrb/files/{YYYY}/{MM}/Order-No.-{N}.pdf`

## How it works

The Board publishes its whole corpus on one server-rendered index page
containing HTML tables of Board *Decisions* and Board *Orders*. Each row
carries the decision/order number, case name, case number, date, and
links to the PDF (plus an `-ADA` accessible copy of the same document).
The scraper:

1. Fetches the index page and parses every table row that links a PDF.
2. Prefers the standard PDF (falls back to the ADA copy).
3. Downloads each PDF and extracts full text with the shared
   `common.pdf_extract` extractor (older scanned decisions go through
   OCR).
4. Records the decision/order number, case name, case number and issue
   date from the index row. `record_id` is the PDF filename stem
   (e.g. `Decision-No-530`), which is stable and unique.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions and orders of the Hawaii Labor Relations Board are official works of Hawaii state government (edicts of a quasi-judicial government body) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
