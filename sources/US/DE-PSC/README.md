# US/DE-PSC — Delaware Public Service Commission (Orders)

Full text of **Orders** issued by the Delaware Public Service Commission (PSC)
adjudicating utility dockets — electric, natural gas, water, wastewater, cable
and telecommunications rate cases, certificates of public convenience and
necessity, § 215 change-of-control filings, renewable / community energy
facility applications, tariff filings and rulemaking.

Each Commission Order is an administrative adjudication / edict of a specific
docket → **`case_law`** (US-DE).

## Source

- **System:** DelaFile e-filing — `https://delafile.delaware.gov`
- **Docket resolver:** `AdvancedSearch/AdvancedSearchDocket.aspx?CNo={base64(YY-NNNN)}`
  → returns the internal `MatterId` GUID.
- **Docket sheet:** `CaseManagement/DocketPage.aspx?MatterNo={docket}&MatterId={guid}`
  → `grdDocumentDetails` grid; rows with Document Type = "Order".
- **Document:** `CaseManagement/ViewFileNetDocument.aspx?Id={guid}` → raw PDF.

> **Note:** the public WordPress site `depsc.delaware.gov` is behind a BIG-IP
> ASM WAF that rejects programmatic requests. DelaFile is not firewalled and is
> the access path used here.

Dockets exist from **2016** onward (the DelaFile era). Procedural orders are
born-digital (clean fitz extraction); larger substantive orders are scanned and
require Tesseract OCR (`pytesseract` + `tesseract` on the VPS).

## Usage

```bash
python bootstrap.py test-api             # connectivity + one full-text order
python bootstrap.py bootstrap --sample   # ~12 sample orders
python bootstrap.py bootstrap-fast       # full pull (VPS), streams to data/
```

## License

[Public Domain — U.S. Government Work (Delaware)](https://www.law.cornell.edu/uscode/text/17/105) — Delaware Public Service Commission Orders are official state government edicts in the public domain. No attribution required; commercial use permitted.
