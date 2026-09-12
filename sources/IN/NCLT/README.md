# IN/NCLT — National Company Law Tribunal

Insolvency and corporate resolution orders from NCLT, India's specialist tribunal for company law and IBC matters.

## Coverage

- **Benches**: 20+ across India (Mumbai, New Delhi, Chennai, Ahmedabad, Bengaluru, Kolkata, Hyderabad, etc.)
- **Case types**: IBC Admissions, Liquidation, Resolution Plans, Dissolution, Appointment of RP/Liquidator
- **Period**: 2017–present
- **Format**: PDF orders (digitally generated, text-extractable)

## Data Access

Uses IBBI's order aggregation portal:
1. Paginate through `https://ibbi.gov.in/orders/nclt?page=N` (20 orders per page).
   The pager reports the corpus size and the last page directly — 31,541 orders
   across 1,578 pages on 2026-09-02.
2. Parse the HTML table for case metadata and the PDF link
3. Download PDFs from `https://ibbi.gov.in/uploads/order/{hash}.pdf`

A full run streams to `data/records.jsonl` and checkpoints each page to
`data/checkpoint.json`, so a slot torn down at the wall-clock cap resumes
instead of restarting at page 1.

### Notes for whoever touches the listing parser next

- The PDF link has changed shape at least once: it used to be the argument of an
  `onclick` handler and is now a plain `<a href=/uploads/order/….pdf download>`
  with an **unquoted** href. Missing the current shape does not error — every
  page parses to "0 entries" while still returning HTTP 200, which reads exactly
  like a datacenter-IP block (issue #1537). Page 1 parsing to zero rows now raises.
- The same order is listed under two paths: the classic
  `/uploads/order/{hash}.pdf` and a newer
  `/uploads/order/{date}-{time}-{slug}-{hash}.pdf`. Both carry the same trailing
  hash, which is what `_id` keys on — roughly 15% of listing rows are such
  relistings and would otherwise land as two documents.
- Not every listing row carries an order file, and the share grows in the older
  pages. Those rows are counted (`skipped_no_pdf`) rather than dropped silently,
  so a run's totals reconcile against the pager's 31,541.
- Bench is derived from the case number (`…/MB/2025` → Mumbai), testing every
  token rather than only the first (which is always the statute tag `IB`). Codes
  come from what the corpus actually uses; `CB` is Cuttack, not Chandigarh,
  because the listing files `IA (IB) No.270/CB/2025 in CP (IB) No. 142/CTB/2019`.

## License

[Government Open Data](https://ibbi.gov.in/) — Indian government tribunal decisions are public records. Attribution recommended.
