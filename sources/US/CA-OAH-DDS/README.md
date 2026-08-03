# US/CA-OAH-DDS — California OAH DDS (Lanterman Act) Decisions

Full text of **DDS / Lanterman Act** "fair hearing" **Decisions** of the
California **Office of Administrative Hearings (OAH)**, General
Jurisdiction Division.

OAH is California's central, independent administrative tribunal. Under the
**Lanterman Developmental Disabilities Services Act**, a consumer with a
developmental disability (or their family/authorized representative) may
appeal a **regional center**'s decision to deny, reduce, or terminate a
service or support. An Administrative Law Judge holds a fair hearing and
issues a **Decision** that resolves each specific contested case — i.e.
**case_law**.

## Source

- Decision library (Sitecore search-list page, server-paginated):
  `https://www.dgs.ca.gov/OAH/Case-Types/General-Jurisdiction/Resources/DDS-Decisions?page={N}`
- Documents: born-digital text-layer PDFs at
  `/-/media/Divisions/OAH/General-Jurisdiction/DDS-Decisions/<name>.pdf`

~**2,550** decisions are indexed (~25 per page, pages 1..~102,
oldest→newest). PDF filenames begin with the 10-digit OAH case number
(YYYYMMNNNN) followed by the `084` DDS agency code and optional suffixes
(`Acc`, `Adopted`, `Revised`, or a consolidated `<case1>-<case2>084`).
Each PDF opens `BEFORE THE OFFICE OF ADMINISTRATIVE HEARINGS STATE OF
CALIFORNIA`, carries `OAH No. <caseno>` and the decision date in the body,
and is extracted via `common.pdf_extract` (no OCR needed). No CAPTCHA, no
auth.

## Fields

`_id`, `_source`, `_type=case_law`, `case_number`, `case_numbers`,
`parties`, `issuer`, `title`, `text` (full decision), `url`, `date`,
`jurisdiction=US-CA`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~2,550 decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Notes

- Unlike the sibling **US/CA-OAH-SpecialEd** source (which uses the
  Sitecore MediaSearch AJAX API), this page's `folderPath` MediaSearch
  parameter is silently ignored, so the plain `?page=N` HTML listing is
  the authoritative enumeration.
- `date` prefers the **last** "Month D, YYYY" in the decision body
  (typically the cover/signature decision date; the first date is usually
  the hearing date).

## License

[Public Domain — US Government Work (California state administrative decisions)](https://www.law.cornell.edu/uscode/text/17/105) — Decisions of the California Office of Administrative Hearings are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
