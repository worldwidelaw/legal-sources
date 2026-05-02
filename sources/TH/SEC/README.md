# TH/SEC — Thailand Securities and Exchange Commission Enforcement

Enforcement actions from the Thailand SEC, including criminal complaints,
administrative fines, civil actions, cases settled, and administrative orders.

## Data Access

Uses the SEC's internal REST API at `market.sec.or.th/public/idisc/api/Enforce/GetEnforces`.
The API accepts POST requests with date range and filter parameters, returning an HTML table
with enforcement records. A CSRF token (`rtk`) is obtained from the main enforcement page.

Records include full summarized facts describing the violation, relevant laws cited,
enforcement type, and case outcome details.

## License

[Thai Government Publication](https://market.sec.or.th/public/idisc/en/Enforce/) — enforcement actions published for public transparency. No restrictions on use.
