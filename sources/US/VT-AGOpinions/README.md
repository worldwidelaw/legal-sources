# US/VT-AGOpinions — Vermont Attorney General Opinions

Full text of formal and informal legal opinions issued by the Office of the
Vermont Attorney General. Each opinion answers a legal question posed by a
public official or agency and is an authoritative (advisory) interpretation of
Vermont law (doctrine).

## Source

- **Publisher:** Office of the Vermont Attorney General
- **Listing page:** https://ago.vermont.gov/about-attorney-generals-office/attorney-general-opinions
- **Format:** One Drupal listing page linking digitally-produced text PDFs hosted
  under `ago.vermont.gov/sites/ago/files/`.
- **Coverage:** ~2000–present for the posted formal and selected informal opinions.

## How it works

1. Fetch the listing page HTML.
2. Extract each opinion anchor (link text + PDF URL). The link text carries the
   opinion type ("Formal Opinion" / "Informal Opinion") and the issue date.
3. Download each PDF and extract its text via the shared OOM-hardened
   `common.pdf_extract.extract_pdf_markdown` helper.
4. Normalize into the standard `doctrine` schema.

### Notes

- `ago.vermont.gov` returns HTTP 403 to plain requests without a same-site
  `Referer`; the scraper sets `Referer` to the listing page and falls back to a
  `curl` subprocess for hosts requiring TLS 1.3.
- A small number of older formal opinions (e.g. FO 2014-1, FO 2008-1) are scanned
  images with no text layer and are skipped (no OCR).

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Sample documents → sample/
python bootstrap.py bootstrap            # Full pull → data/records.jsonl
```

## License

[Public Domain (US Government Work — Vermont)](https://www.law.cornell.edu/uscode/text/17/105) — Vermont Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
