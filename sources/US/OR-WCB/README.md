# US/OR-WCB — Oregon Workers' Compensation Board, Board Orders

Orders of the Oregon Workers' Compensation Board, the agency that reviews
Administrative Law Judge orders in workers' compensation claims under ORS
chapter 656. Board orders are Oregon's workers' compensation case law and are
published in the Van Natta reporter (`77 Van Natta 579 (2025)`).

- **Site:** https://www.oregon.gov/wcb/board-orders/Pages/index.aspx
- **Coverage:** ~28,000 orders, 1996–present
- **Type:** `case_law`
- **Auth:** none

## What is in the corpus

One record per order, across every category the Board publishes:

| Path segment | Order kind |
|---|---|
| `review` | Orders on Review (the main appellate output) |
| `recon`, `remand` | Orders on Reconsideration / on Remand |
| `omo` | Own Motion orders (post-aggravation-rights claims) |
| `cda` | Claim Disposition Agreement approvals, abatements, reconsiderations |
| `tpo` | Third Party Distribution orders (ORS 656.593) |
| `miscellaneous` | Dismissals, abatements, procedural orders |
| `cv`, `osha` | Civil penalty and OSHA contested-case orders (1990s) |

Most orders are born-digital PDFs; the legacy years are `<PRE>`-formatted HTML.
Both extract to clean full text (the samples run 644–11,528 characters). The
SharePoint migration rewrote every legacy `.htm` order with an
`<!--[if gte mso 9]><xml><mso:CustomDocumentProperties>` block in the head, so
`html_to_text` strips comments and `<head>` before tag-stripping — otherwise the
migration bot's field values land in front of the order text.

## Access path

There is no directory listing — `/wcb/Orders/Forms/AllItems.aspx` is `401` —
and the order finder at `/wcb/board-orders/Pages/board-review.aspx` ships an
empty table that its own JavaScript fills client-side. That script reads the
SharePoint list `Orders` over **SOAP** (`jquery.SPServices` →
`_vti_bin/Lists.asmx`), which answers anonymous callers `401 UNAUTHORIZED` with
`WWW-Authenticate: NTLM` at both `/wcb/` and `/wcb/board-orders/`.

The **REST endpoint on that same list is anonymous-readable**, and that is what
this scraper uses:

```
GET /wcb/_api/web/lists/getbytitle('Orders')/items
      ?$select=Id,Title,FileRef,WCBYear,WCBOrderType,WCBClaimantName,
               WCBCase,WCBDateOrderIssued,WCBVanNattaVolume,WCBVanNattaPage
      &$filter=WCBYear eq 2024
      &$top=1000
Accept: application/json;odata=nometadata
```

It is the authoritative index: one row per order carrying `FileRef` (the
document's server-relative path) plus exactly the metadata the finder displays —
claimant, WCB case number, issue date, order type and Van Natta volume/page.
Folder rows have a null `WCBYear`, so filtering by year yields file rows only.
The list reports `ItemCount` ≈ 29,800; per-year counts run ~1,000/year in the
late 1990s down to ~300/year today.

The crawl is partitioned by year — 1996 to the current year, the range the
finder's own `LoadYearSelector` offers — and completed years are checkpointed to
`data/or_wcb_checkpoint.json`, so a killed run resumes instead of re-walking
years it already wrote.

Note that `WCBYear` is the *case* year, not always the decision year: an order
under `/wcb/Orders/1996/review/` can carry `WCBDateOrderIssued` of 1998 (the
order's own "Entered at Salem" line agrees). Partitioning is still exhaustive
because every row has exactly one `WCBYear`.

## Not included

`/wcb/board-orders/Documents/vn-archive/` holds 83 scanned Van Natta volumes
covering 1967–2001, each 40–48 MB. They are bound quarterly reporters, not
individual orders ("not linked to individual orders", per the agency), and the
1996–2001 orders they contain are already in this source as individual files.

`/wcb/board-orders/Documents/court-orders/` holds Oregon Court of Appeals and
Supreme Court decisions in workers' compensation appeals — appellate court
output, not Board output.

## Usage

```bash
python bootstrap.py test-api            # index reachable + one order extracts
python bootstrap.py bootstrap --sample  # 15 orders spread over the full range
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # full pull (VPS wrapper alias)
```

## Record shape

```json
{
  "_id": "OR-WCB-2024-review-jan-2203168",
  "_source": "US/OR-WCB",
  "_type": "case_law",
  "title": "Mayta, Tamera M, Order on Review",
  "text": "In the Matter of the Compensation of\nTAMERA M. MAYTA, Claimant ...",
  "date": "2024-01-04",
  "url": "https://www.oregon.gov/wcb/Orders/2024/review/jan/2203168.pdf",
  "court": "Oregon Workers' Compensation Board",
  "jurisdiction": "US-OR",
  "docket_number": "22-03168",
  "case_numbers": ["22-03168"],
  "parties": "Mayta, Tamera M",
  "order_type": "Order on Review",
  "citation": "76 Van Natta 1 (2024)",
  "volume": 76,
  "page": 1,
  "year": 2024,
  "source_format": "pdf",
  "language": "en"
}
```

The list row wins wherever it is populated: `date` is `WCBDateOrderIssued`,
`parties` is `WCBClaimantName`, `case_numbers` is `WCBCase` (comma-separated for
consolidated dockets) and `citation` is `WCBVanNattaVolume`/`Page`. Only orders
selected for the reporter carry a Van Natta cite, so `citation` is null for most
CDA, Crime Victim and OSHA orders. The text parsers kept from the previous
revision remain as fallbacks for rows the Board left blank: the "Entered at
Salem, Oregon on …" line for `date`, the caption for `parties`, and the running
head — never the body, which quotes other decisions' cites — for `citation`.

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — orders of a state adjudicative body are government edicts and are not subject to copyright. No attribution required; commercial use permitted.
