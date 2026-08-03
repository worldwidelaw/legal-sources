# US/SEC-AdminProceedings — SEC Administrative Proceedings

Full text of the U.S. **Securities and Exchange Commission's** published
**Administrative Proceeding** documents — orders instituting proceedings,
ALJ initial decisions, Commission opinions and orders, settlement orders,
and related determinations issued in the SEC's administrative enforcement
and regulatory adjudications.

Each release advances or resolves a specific administrative proceeding
against named respondents = **case_law**.

## Source

- **List (paginated):** https://www.sec.gov/enforcement-litigation/administrative-proceedings?page=N
- Each row links to the document PDF at
  `/files/litigation/admin/{YYYY}/{release}[-suffix].pdf` and shows the
  SEC Release No. (e.g. `34-105843`).

## Build recipe

1. Walk the pager from `page=0` until an empty page.
2. For each `<tr class="pr-list-page-row">`, read the `<time datetime>`
   publish date, the respondent (anchor text), the PDF URL, and the SEC
   Release No.
3. Download each PDF and extract full text with the shared
   `common.pdf_extract` extractor (born-digital PDFs have a clean text
   layer; older scans fall back to OCR).
4. `record_id` = the PDF filename stem (stable, unique).

No auth, no CAPTCHA — builds locally. **Note:** sec.gov requires a
descriptive `User-Agent` identifying the requester (SEC automated-access
policy); the scraper sends one.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain — U.S. federal government work (17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — SEC administrative-proceeding documents are works of a U.S. federal agency and its administrative law judges and are not subject to copyright. Free to use, including commercially.
