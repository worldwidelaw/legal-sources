# US/LA-PSC — Louisiana Public Service Commission Orders

Full text of **Orders** issued by the **Louisiana Public Service Commission
(LPSC)** adjudicating utility and motor-carrier dockets across the electric,
natural-gas, water, telecommunications and transportation industries. Each Order
is an administrative adjudication of a specific docket by the Commission —
**case_law**.

## Source

- **Publisher:** Louisiana Public Service Commission (LPSC)
- **Public portal:** https://lpscpubvalence.lpsc.louisiana.gov/
- **Type:** `case_law`
- **Auth:** none (public)

## How it works

`lpscpubvalence.lpsc.louisiana.gov` is the public face of the LPSC **STAR**
case-management system — an ASP.NET MVC app whose Kendo UI grids are backed by
plain JSON endpoints under `/portal/PSC/`.

1. `POST /portal/PSC/OrderSearch` (a Kendo `aspnetmvc-ajax` grid read) with a
   date window (`paramSet[StartDate]` / `paramSet[EndDate]` as `M/D/YYYY`) plus
   the standard grid paging params (`sort=`, `page=`, `pageSize=`, `group=`,
   `filter=`). The server returns `{"Data": [...], "Total": N}`: one row per
   Order carrying `OrderId`, `DocumentNumber` (the Order number, e.g.
   `U-37595`, `02-2020`, `S-37857`), `OrderDate` (`"/Date(ms)/"`),
   `Description`, `Synopsis` and the associated `Dockets`
   (`[{MatterNumber, MatterId}]`).
   > The grid's `sort` must be sent as an empty scalar (`sort=`), **not**
   > bracket-indexed (`sort[0][field]=…`) — the latter returns a server 500.
2. `fetch_all()` walks month windows newest-first to the ~1990 floor, paging
   `pageSize=100` until `Total` is reached (~400–700 Orders/year).
3. Each Order's document page is `/portal/PSC/DocumentDetails?documentId={OrderId}`;
   it embeds the download link `/portal/PSC/ViewFile?fileId={opaque-encoded-id}`.
   `GET` that link streams the born-digital Order PDF.
4. Full text is extracted from the PDF via `fitz`/PyMuPDF (Tesseract OCR
   fallback for the rare image-only scan).

## Usage

```bash
python bootstrap.py test-api             # connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — Louisiana)](https://www.law.cornell.edu/uscode/text/17/105) — Louisiana Public Service Commission Orders are official state government edicts in the public domain. No attribution required; commercial use permitted.
