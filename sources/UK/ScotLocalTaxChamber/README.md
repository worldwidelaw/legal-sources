# UK/ScotLocalTaxChamber — First-tier Tribunal for Scotland Local Taxation Chamber

Written decisions of the **First-tier Tribunal for Scotland Local Taxation
Chamber** (LTC), the devolved Scottish tribunal (established **1 April 2023**)
that decides Scottish local-taxation appeals:

- **Council tax** — banding, liability, discounts/exemptions, penalties, and
  council-tax-reduction reviews;
- **Non-domestic (business) rates** and **rateable-value / valuation-roll**
  appeals;
- **Water and sewerage charge** appeals.

The LTC absorbed the functions of the former **Valuation Appeal Committees** and
the **Council Tax Reduction Review Panel**. Its administration is provided by the
**Scottish Courts and Tribunals Service (SCTS)**.

These are adjudicative **case law** for the **Scotland (GB-SCT)** jurisdiction.
They are **not** covered by `UK/CaseLaw` (which indexes England & Wales superior
courts and reserved UK tribunals via the National Archives Find Case Law
service; LTC decisions are not on that service). Distinct from
`UK/ScotTaxChamber`, which covers the devolved *national* taxes (LBTT / SLfT).

## Source

- **Site:** https://www.localtaxationchamber.scot/
- **Coverage:** ~1,180 decisions, 2023–present (Chamber Ref `FTS/LTC/XX/YY/NNNNN`)
- **Auth:** none (free public access)

## How it works

The site is a React single-page app whose "Decisions" view is backed by an
**Azure Blob Storage** account. The decision PDFs live in a publicly-listable
container:

```
https://ltcpastrauks003.blob.core.windows.net/decision-documents
```

(the storage-account and container names are read straight from the app's own
runtime config baked into the JS bundle; the container permits anonymous
blob-list + blob-read, with no SAS token required).

The scraper:

1. Lists the container via the Azure *list container* REST call
   (`?restype=container&comp=list`), following `NextMarker` if present.
2. Downloads each born-digital decision PDF
   (`Decision (Appeal) 23.00012.pdf`, `Decision (Upper Tribunal Referral) …`)
   and extracts full text with PyMuPDF (pdfplumber/pypdf fallback). No OCR is
   required.
3. Parses the Chamber Ref (`FTS/LTC/XX/YY/NNNNN`), parties (appellant /
   respondent), tribunal member and decision date from the PDF text; the
   decision type (Appeal / Review / Upper Tribunal Referral / Expenses Request)
   comes from the blob filename.

One record per decision PDF.

```
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent decisions)
python bootstrap.py test               # Connectivity/extraction test
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[SCTS website terms of use](https://www.scotcourts.gov.uk/terms-of-use) — the
Scottish Courts and Tribunals Service permits reproduction of judgments and
decisions for personal and in-house use, but restricts commercial re-use without
consent. Same basis as `UK/ScotHousingChamber` and `UK/ScotTaxChamber`. Not
published under the Open Government Licence.
