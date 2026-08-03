# US/NM-TaxDecisions — New Mexico TRD Decisions & Orders

Full text of the **Decisions & Orders** issued by the New Mexico Taxation
and Revenue Department's Administrative Hearings Office.

- **Source page:** https://www.tax.newmexico.gov/all-nm-taxes/tax-decisions-orders/
- **Type:** `case_law` (hearing-officer adjudications of a contested case)
- **Jurisdiction:** US-NM
- **Auth:** none

## What a Decision & Order is

A written Decision and Order issued by an Administrative Hearings Office
hearing officer resolving a taxpayer's protest of a TRD assessment or denial.
These are adjudications of a contested case. (The TRD's interpretive
*Rulings* are a separate `doctrine` collection, `US/NM-TaxRulings`.)

## How it works

The page embeds the same **RealFile** (rtsclients.com) file-browser widget
used by `US/NM-TaxRulings` (same `accountGUID`, different folder/widget). The
folder tree and files are served by a public JSON API:

```
GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/GetWidgetFiles
    ?widgetId=...&folderId=...&rootFolderId=...&accountGUID=...&publicTokenGUID=...
```

The root folder holds one subfolder per **year** (1994 → present); each year
folder holds the decision PDFs, named like `25-04 Raymond Merrick.pdf`
(decision number + taxpayer caption — older years use underscores, e.g.
`12-04_california_closets.pdf`). The scraper walks every year folder, dedups
files by `fileId`, and downloads each born-digital PDF from:

```
GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/PublicFiles/{accountGUID}/{fileId}/{fileName}
```

Full text lives only in the PDF, extracted via the shared OOM-hardened
`common.pdf_extract` helper (pdfplumber → pypdf → OCR fallback). A <200-char
guard skips the rare image-only/empty scan.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Decisions and Orders of the New Mexico Taxation and Revenue Department /
Administrative Hearings Office are official state-government works in the
public domain under the government-edicts doctrine. Commercial use permitted;
no attribution required.
