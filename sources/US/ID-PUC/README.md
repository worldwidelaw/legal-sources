# US/ID-PUC — Idaho Public Utilities Commission Orders

Full text of **Orders** issued by the **Idaho Public Utilities Commission (IPUC)**
adjudicating utility cases: electric, natural gas, water, telecommunications and
railroad rate cases, certificates of public convenience and necessity, integrated
resource plans, PURPA / avoided-cost dockets, fuel and power-cost adjustments,
tariff filings and rulemaking. Each Commission Order is an administrative
adjudication of a specific case — classified as **case_law**.

- **Coverage:** ~4,900 unique orders, 1989–present.
- **Jurisdiction:** US-ID (Idaho).
- **Type:** case_law.

## Access

`puc.idaho.gov` firewall-drops all traffic from non-Idaho / datacenter vantages
(no ICMP, TCP 443 filtered; HTTP 000 from every build vantage and from Anthropic's
fetch vantage as of 2026-07-23). The corpus is therefore read from the **Internet
Archive (Wayback Machine)**, which has a substantial crawl of the Commission's
public Fileroom order PDFs.

1. `fetch_all()` enumerates archived `.../OrdNotc/*.pdf` snapshots via the Wayback
   CDX API (case-insensitive urlkey filter, `statuscode:200`, one snapshot per URL).
   Two historical path schemes exist:
   - new: `/Fileroom/PublicFiles/{TYPE}/{UTIL}/{CASESEG}/OrdNotc/{file}.pdf`
   - old: `/fileroom/cases/{type}/{util}/{caseseg}/ordnotc/{file}.pdf`
2. Only genuine Orders are kept (filename carries an `Order No NNNNN`); procedural
   "Notice of ..." documents are dropped. Records are de-duplicated on
   `(case number, order number)`, preferring the final / amended order and the
   newer Fileroom scheme.
3. `normalize()` downloads the raw archived PDF
   (`https://web.archive.org/web/{ts}id_/{original}`) and extracts full text via
   fitz/PyMuPDF (Tesseract OCR fallback for the rare scanned order). The case number
   is reconstructed from the path segment (`IPCE2603` → `IPC-E-26-03`); the order
   date is parsed from the filename `YYYYMMDD` prefix, falling back to the PDF body
   "Service Date".

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample orders
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — Idaho)](https://www.law.cornell.edu/uscode/text/17/105)
— Idaho Public Utilities Commission Orders are official state government edicts in
the public domain. Commercial use permitted; no attribution required.
