# US/SD-PUC — South Dakota Public Utilities Commission Orders

Full text of **Orders** issued by the South Dakota Public Utilities Commission
(PUC) adjudicating utility dockets across the **electric**, **natural-gas**,
combined **gas/electric** and **telecommunications** industries. Each Order is
an administrative adjudication of a specific docket by the Commission =
`case_law`.

## Source

- **Official site:** https://puc.sd.gov/Dockets/
- **Access:** public HTML docket archive + born-digital Order PDFs, no auth.

## How it works

1. Each docket industry has a landing page `/Dockets/{Type}/default.aspx`
   (Type ∈ Electric, NaturalGas, GasElectric, Telecom) that lists year
   sub-pages `{YYYY}/default.aspx`.
2. Each year page lists that year's docket pages `{DOCKET}.aspx`
   (e.g. `EL24-001.aspx`, `NG24-003.aspx`, `TC24-002.aspx`, `GE24-001.aspx`).
3. Each docket page carries an **"Orders:"** section — a list of
   `MM/DD/YY - description` links to born-digital Order PDFs under
   `/commission/dockets/...`. These are isolated from the "Filed Documents"
   section (party filings, exhibits, data requests), which is excluded.
4. `normalize()` downloads each Order PDF and extracts the full text with
   PyMuPDF (Tesseract OCR fallback for the rare image-only scan).

The corpus spans **2000 to present**.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap-fast       # Full pull (VPS)
```

## License

[Public Domain (US Government Work — South Dakota)](https://www.law.cornell.edu/uscode/text/17/105) — South Dakota Public Utilities Commission Orders are official state government edicts in the public domain. Commercial use permitted; no attribution required.
