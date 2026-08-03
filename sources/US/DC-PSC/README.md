# US/DC-PSC — Public Service Commission of the District of Columbia (Orders)

Full text of **Orders** issued by the Public Service Commission of the District
of Columbia (DCPSC) adjudicating formal cases and utility dockets across the
electric, natural-gas and telecommunications industries — rate cases,
certificate/merger applications, complaints and rulemakings. Each Commission
Order is a numbered administrative adjudication of a specific matter =
**case_law**.

The Order series is sequentially numbered and runs from **Order No. 1
(1913-03-11)** through the present (~22,900 Orders), a deep historical corpus of
DC utility case law.

## Source

- **Portal:** https://edocket.dcpsc.org/ (DCPSC E-Docket System, an Angular SPA)
- **API:** `https://edocket.dcpsc.org/apis/api` (public REST, no auth)
- **Type:** case_law
- **Jurisdiction:** US-DC (District of Columbia)

## How it works

1. **Newest Order** — page `Filing/GetFilings` (ordered by `receivedDate` desc)
   until the first row whose `isOrder` flag is set; its `order_number` is the
   current maximum.
2. **Walk the series** — for every Order number `N` down to 1,
   `GET Filing/GetFilings?orderNumber=N` returns the filing carrying that Order.
   The non-confidential `isOrder` row exposes `attachmentId` and `attachment`
   (a GUID `<guid>.pdf` for modern Orders, or an archived `OA/<n>.pdf` for the
   pre-2001 scanned back-catalogue), plus received date, description, company
   and docket number.
3. **Download & extract** —
   `GET Filing/download?attachId=<id>&guidFileName=<attachment>` streams the
   Order PDF (public, no auth). Full text is extracted with fitz/PyMuPDF; the
   older scanned Orders already carry an embedded OCR'd text layer, and the rare
   image-only page falls back to Tesseract OCR.

Confidential filings (`attachment == "Confidential"` or `isConfidential`) are
skipped — their public PDF is not served. Admin endpoints
(`Order/GetOrders`, `Case/GetAllCases`) are 401-gated and are **not** used.

## Usage

```bash
python bootstrap.py test-api            # connectivity + full-text check
python bootstrap.py bootstrap --sample  # ~12 sample Orders
python bootstrap.py bootstrap           # full pull (all Orders)
python bootstrap.py bootstrap-fast      # high-throughput full pull (VPS)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— DCPSC Orders are official government edicts of the District of Columbia and
are in the public domain. Commercial use permitted; no attribution required.
