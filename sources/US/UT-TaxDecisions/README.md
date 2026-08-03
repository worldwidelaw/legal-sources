# US/UT-TaxDecisions — Utah State Tax Commission

Commission Decisions (case_law) and Private Letter Rulings (doctrine) issued
by the Utah State Tax Commission Appeals Unit.

- **Commission Decisions** — redacted adjudicative decisions resolving a
  taxpayer's appeal of a Tax Commission assessment or denial → `case_law`.
- **Private Letter Rulings** — interpretive guidance the Commission issues in
  response to a taxpayer request → `doctrine`.

## Source

- Decisions index: <https://tax.utah.gov/commission/decisions/>
- Rulings index: <https://tax.utah.gov/commission/rulings/>
- File CDN: `https://files.tax.utah.gov/tax/commission/{decision,ruling}/{NUM}.pdf`

Both index pages are WordPress pages whose **Ninja Tables ship every row in
the server-rendered HTML** — no JavaScript, no CAPTCHA, no auth. Each
`<tr class="ninja_table_row_N">` carries a direct PDF link plus metadata
columns (number, decision/ruling date, tax type, tax year, posted date).
Full text lives only in the born-digital PDF and is extracted with the shared,
OOM-hardened `common.pdf_extract` helper.

## Coverage

The server-rendered index exposes ~184 recent Commission Decisions and ~17
Private Letter Rulings (201 documents total). The full historical decision set
(1984–present) is reachable via the deterministic `YY-NNNN` numbering and the
site's client-side search; the recent index is sufficient for the initial
build and can be extended by HEAD-probing the numbering per year.

## Build / vantage note

Discovery (HTML row parsing) is verified working locally — 201 documents with
dates and numbers parsed. **The file CDN `files.tax.utah.gov` must be reachable
to fetch the PDFs.** From some egress vantages the CDN returns a generic HTML
404 for every path; the scraper guards against ingesting that page as a PDF
(checks `Content-Type`/`%PDF-` magic). Launch from a residential/VPS vantage
that can fetch `files.tax.utah.gov`, then confirm full text before marking the
source complete.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction check
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain (US Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — Commission Decisions and Private Letter Rulings of the Utah State Tax Commission are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
