# US/TN-TPUC — Tennessee Public Utility Commission — Orders

Full-text **Orders** issued by the **Tennessee Public Utility Commission**
(TPUC, formerly the **Tennessee Regulatory Authority / TRA**), the state
agency that regulates investor-owned utilities and adjudicates the dockets
before it: rate cases, certificates of convenience and necessity (CCN),
service-territory amendments, tariff/franchise matters, consumer complaints,
protective orders, and other contested-case rulings. Each Commission Order is
an administrative adjudication of a specific docket → `case_law`.

## Source

- **Electronic Docket File Room:** https://tpucdockets.tn.gov/
- Each docket has a static HTML page `/dockets/{DDDDDDD}.htm` where the 7-digit
  docket number is a 2-digit filing year + 5-digit sequence (e.g. `2300051` =
  Docket **23-00051**).
- Filing PDFs live at `/archive/filings/{YYYY}/{DDDDDDD}{suffix}.pdf`.

## Method

1. Enumerate docket pages per filing-year prefix (newest first) with a
   consecutive-miss gap tolerance (a missing docket returns HTTP 403).
2. On each docket page, keep the filing rows whose **Company Filing** column is
   the agency ("Tennessee Public Utility Commission" / "Tennessee Regulatory
   Authority") **and** whose Description contains "Order" — this isolates
   Commission-issued orders and excludes party filings (motions, proposed
   orders, witness lists).
3. Download the filing PDF and extract text with PyMuPDF/`fitz`.
4. Drop older scanned-image orders with no OCR text layer
   (body < 500 chars).

Born-digital orders (the e-filing era, roughly 2000+) extract clean full text
(observed ~6K–260K chars/order).

## Usage

```bash
python bootstrap.py test-api             # connectivity check
python bootstrap.py bootstrap --sample   # ~12 sample orders
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```

Requires `PyMuPDF` (`fitz`) for PDF text extraction.

## License

[Public Domain (US state government edict)](https://www.law.cornell.edu/uscode/text/17/105) — Orders of the Tennessee Public Utility Commission are edicts of a US state government body and are not subject to copyright (government edicts doctrine). Freely reusable, including commercially. No attribution required.
