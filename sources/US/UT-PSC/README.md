# US/UT-PSC — Utah Public Service Commission Orders

Full text of **Orders** issued by the **Public Service Commission of Utah (PSC)**
adjudicating utility dockets (electric, natural gas, water, telecommunications).
Each Order is an administrative adjudication / disposition of a specific docket
by the Commission = **case_law**.

## Source

- **Site:** https://psc.utah.gov/ (WordPress)
- **Document store:** https://pscdocs.utah.gov/ (S3 bucket)
- **Coverage:** ~4,700 dockets; thousands of Orders.

## How it works

1. Enumerate every docket page from the WordPress sitemap
   (`/wp-sitemap-posts-post-{1,2,3}.xml`). Each docket is a post at
   `/YYYY/MM/DD/docket-no-{NN-NNN-NN}/`.
2. GET each docket page. It lists every filing as an anchor whose text is the
   document description and whose href is a born-digital PDF on
   `pscdocs.utah.gov`. Order-type filings are the anchors whose text matches a
   whole-word "order" (`Order Approving...`, `Scheduling Order...`,
   `Order Granting...`).
3. Download each Order PDF and extract full text (fitz/PyMuPDF; Tesseract OCR
   fallback for the rare image-only scan).

### Vantage note

`pscdocs.utah.gov` (S3) serves objects publicly to residential clients but
returns **HTTP 403 to cloud/datacenter IP ranges** (verified from the build
vantage and the WebFetch egress). To stay vantage-independent, `_download()`
tries the live `pscdocs` URL first and, on failure, falls back to the
**Internet Archive Wayback Machine** (~20k `pscdocs` PDFs captured, reachable
from any vantage). From a residential / proxied vantage the live path retrieves
the full corpus.

## Usage

```bash
python bootstrap.py test-api            # connectivity / structure check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # high-throughput full pull (VPS)
```

## Fields

`doc_id`, `docket_number`, `industry`, `title`, `text` (full Order text),
`date`, `url`, `docket_url`, `jurisdiction` (US-UT).

## License

[Public Domain (US Government Work — Utah)](https://www.law.cornell.edu/uscode/text/17/105) —
Utah Public Service Commission Orders are official state government edicts in
the public domain. No attribution required; commercial use permitted.
