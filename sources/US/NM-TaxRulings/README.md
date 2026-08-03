# US/NM-TaxRulings — New Mexico Taxation and Revenue Department (Rulings)

Full text of the interpretive **Rulings** published by the New Mexico
Taxation and Revenue Department (TRD).

- **Source page:** https://www.tax.newmexico.gov/all-nm-taxes/rulings/
- **Type:** `doctrine` (official interpretive guidance, not adjudications)
- **Jurisdiction:** US-NM
- **Auth:** none

## What a Ruling is

A Ruling is a written statement of the Department's interpretation of how
New Mexico's tax laws and regulations apply to a stated set of facts. They
are official state-government interpretive guidance. (The TRD's adjudicatory
hearing-officer *Decisions & Orders* are a separate `case_law` collection.)

## How it works

The Rulings page embeds a **RealFile** (rtsclients.com) file-browser widget.
Its folder tree and files are served by a public JSON API:

```
GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/GetWidgetFiles
    ?widgetId=...&folderId=...&rootFolderId=...&accountGUID=...&publicTokenGUID=...
```

The scraper recursively walks the folder tree from the root folder
(categories like `400-Gross Receipts and Compensating Tax Act`, each with
tax-type subfolders), dedups files by `fileId`, and downloads each
born-digital PDF from:

```
GET https://klvg4oyd4j.execute-api.us-west-2.amazonaws.com/prod/PublicFiles/{accountGUID}/{fileId}/{fileName}
```

Full text lives only in the PDF, extracted via the shared OOM-hardened
`common.pdf_extract` helper (pdfplumber → pypdf → OCR fallback). A
<200-char guard skips the rare image-only/empty scan.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Rulings of the New Mexico Taxation and Revenue Department are official
state-government works in the public domain under the government-edicts
doctrine. Commercial use permitted; no attribution required.
