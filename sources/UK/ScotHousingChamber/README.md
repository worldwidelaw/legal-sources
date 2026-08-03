# UK/ScotHousingChamber — First-tier Tribunal for Scotland (Housing and Property Chamber)

Written Decisions (with Statement of Reasons) of the **Housing and Property
Chamber of the First-tier Tribunal for Scotland**, the statutory tribunal that
determines disputes between landlords, tenants, homeowners, property factors and
letting agents under Scottish housing law (Private Housing (Tenancies)
(Scotland) Act 2016, Housing (Scotland) Act 1988, Property Factors (Scotland)
Act 2011, the Letting Agent Code of Practice, and related legislation).

These decisions are binding on the parties and appealable to the Upper Tribunal
for Scotland — i.e. **adjudicative case law for the GB-SCT jurisdiction**, which
is *not* covered by `UK/CaseLaw` (England & Wales + reserved UK tribunals only).

## Corpus

| Category (Drupal listing)            | approx. decisions |
|--------------------------------------|-------------------|
| Evictions & civil proceedings        | ~16,300           |
| Other private-tenancy applications   | ~2,300            |
| Property factors                     | ~1,400            |
| Right of entry                       | ~960              |
| Rent (terms / prescribed property costs) | ~640          |
| Letting agents (Code of Practice)    | ~340              |
| **Total**                            | **~21,000+** (2017–present) |

## Access & method

Publisher: Scottish Courts and Tribunals Service (SCTS),
`housingandpropertychamber.scot` (a Drupal site).

1. Discover the six category listing URLs from `/previous-tribunal-decisions`
   (hard-coded canonical paths as a fallback).
2. Page each listing table (`?page=N`, Drupal 0-indexed). Each row carries the
   Chamber reference (`FTS/HPC/.../YY/NNNN`), hearing date (`<time datetime>`),
   parties and one or more decision **PDF** links under `/sites/default/files/`.
3. Download each **born-digital** PDF and extract full text with PyMuPDF
   (`pdfplumber`/`pypdf` fallback). No OCR required.

One record per decision row; multiple attachment PDFs on a row are extracted and
concatenated. The decision date is the row's hearing date, falling back to the
last dated line in the decision text.

### Note on local testing
The host requires a modern TLS stack. macOS system `/usr/bin/python3` (LibreSSL
2.8.3) is rejected with `TLSV1_ALERT_PROTOCOL_VERSION`; test with a modern
OpenSSL build (e.g. Homebrew `python3.11`). The fleet VPS (Linux/OpenSSL) is
unaffected.

## Usage

```bash
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent decisions)
python bootstrap.py test               # Connectivity + extraction check
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[SCTS site terms — personal / in-house use only](https://www.housingandpropertychamber.scot/terms-and-conditions)
— the site states material "may be reproduced without formal permission or
charge for **personal or in-house use only**." The underlying tribunal decisions
are public records (Crown copyright / SCTS). Commercial re-use is flagged as
restricted per project policy.
