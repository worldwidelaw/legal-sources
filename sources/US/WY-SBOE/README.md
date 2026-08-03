# US/WY-SBOE — Wyoming State Board of Equalization (Board Decisions & Orders)

Full-text scraper for the published adjudicative decisions and procedural
orders of the **Wyoming State Board of Equalization (SBOE)** — the
constitutional full-time board that hears and decides all Wyoming state and
local tax appeals: Department of Revenue determinations on sales/use,
mineral/severance, and ad valorem valuation, plus appeals from County Boards of
Equalization on property-tax valuation. Every published document adjudicates a
specific case → `case_law`, jurisdiction `US-WY`.

## Source

- **Board Decisions:** http://taxappeals.state.wy.us/18_opinions.html
- **Procedural Orders:** http://taxappeals.state.wy.us/18_proc_orders.html

Both are server-rendered HTML tables (no JavaScript, no CAPTCHA, no auth). Each
`<tr>` supplies the Petitioner, the decision Date, and the Docket No. linked to
a PDF under `images/` (dominant pattern `images/docket_no_{YYYYNN}.PDF`).

> **Host note:** the site's HTTPS certificate is name-invalid, so the scraper
> talks plain HTTP (`http://taxappeals.state.wy.us/`) with `verify=False`.

## Full text

The decision PDFs are **scanned images with no text layer**. Full text is
extracted via the OCR fallback in `common.pdf_extract` (PyMuPDF → pytesseract),
which requires the `tesseract` binary on the host (export `PATH` to include it
if it is installed off-`PATH`). A `<200`-char guard skips any PDF that still
yields no text.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap --full    # full pull (~1,150 documents)
python bootstrap.py bootstrap-fast      # alias for the full pull (VPS wrapper)
```

## License

[Public Domain — US Government Work (17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — decisions and orders of the Wyoming State Board of Equalization are official Wyoming state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
