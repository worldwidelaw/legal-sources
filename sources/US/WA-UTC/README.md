# US/WA-UTC — Washington Utilities & Transportation Commission Orders

Full text of **Orders** issued by the Washington Utilities and Transportation
Commission (UTC) adjudicating utility dockets across the electric,
natural-gas, water, telecommunications, solid-waste and transportation
(auto-transportation, commercial ferry, pipeline) industries. Each Order
resolves or advances a specific docket — rate case, tariff filing, complaint,
penalty, certificate, or rulemaking — and is an administrative adjudication of
that docket (**case_law**).

## Source

- **Docket system:** https://www.utc.wa.gov/documents-and-proceedings/dockets
  (Drupal 10; the Orders tab of each docket at
  `/casedocket/{year}/{docket}/orders`)
- **Document API:** `https://apiproxy.utc.wa.gov/cases/GetDocument?docID={id}&year={y}&docketNumber={d}`
  (direct born-digital Order PDFs)
- **Auth:** none

## How it works

1. The docket list (`/documents-and-proceedings/dockets`, 50/page, newest
   docket first, ~729 pages back to the 1990s) enumerates every docket; each
   row links to `/casedocket/{year}/{docket}`.
2. Each docket's Orders tab (`/casedocket/{year}/{docket}/orders`) is a
   server-rendered HTML table (no auth, no JS) listing every Order document:
   issue date, a direct born-digital PDF link on `apiproxy.utc.wa.gov`,
   document type, description and file size, plus docket metadata (company,
   filing type, case status, summary).
3. Each Order PDF is downloaded and its full text extracted with fitz/PyMuPDF
   (Tesseract OCR fallback for the rare image-only scan).

**Note:** proof-of-service / affidavit attachments are mis-filed under the
"Order" document types and are filtered out (by filename and a text-head
safety net) so the corpus is not inflated with affidavit near-duplicates.
Pre-~2004 orders published as WordPerfect/Word (`.wpd`/`.doc`) are skipped
(not extractable here).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + full-text check
python bootstrap.py bootstrap --sample   # ~12 sample Orders
python bootstrap.py bootstrap            # Full pull (all Orders)
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Orders of the Washington Utilities and Transportation Commission are official
edicts of a U.S. state government body and are in the public domain. Commercial
use permitted; no attribution required.
