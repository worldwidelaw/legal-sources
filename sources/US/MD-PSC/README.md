# US/MD-PSC — Maryland Public Service Commission (Orders)

Full text of **Orders** issued by the Maryland Public Service Commission
(PSC), the state regulator of electric, gas, water/sewer, telecommunications
and passenger-transportation utilities.

Each Commission Order adjudicates a specific docketed case — rate cases,
certificates of public convenience and necessity (CPCN), merger approvals and
merger-condition enforcement, tariff filings, fuel/purchased-gas adjustments
and regulatory dockets — so each Order is treated as **case_law** (an
administrative adjudication / government edict).

## Access

Orders are published through the Commission's Document Management System (DMS)
portal:

- Listing/search: `https://webpscxb.pscmaryland.com/DMS/commissionorders`
  (ASP.NET WebForms, "Find by date range" search).
- The scraper enumerates orders one calendar month at a time (the date-range
  search returns nothing for spans wider than ~one month), collecting each
  order's DMS mail-log id, order number and issue date.
- Full text is retrieved via a two-hop chain:
  1. `/DMS/maillogpdfview/MailLog/0/0/{maillog_id}/0` → HTML viewer that embeds
     the real document path in `data-pdf='/DMS/pdfview/...'`.
  2. `/DMS/pdfview/{path}` → born-digital `application/pdf`, extracted with
     `fitz` / PyMuPDF.

No authentication is required.

## Fields

`order_number`, `case_number`, `title`, `text` (full order text), `date`
(issue date, ISO 8601), `url` (DMS viewer URL), plus the standard `_id`,
`_source`, `_type`, `_fetched_at`.

## License

[Public Domain — U.S. Government Work](https://www.law.cornell.edu/uscode/text/17/105) — Maryland Public Service Commission Orders are official state government edicts in the public domain; no attribution required, commercial use permitted.

## Usage

```bash
python bootstrap.py test-api             # connectivity + full-text check
python bootstrap.py bootstrap --sample   # ~12 sample orders
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```
