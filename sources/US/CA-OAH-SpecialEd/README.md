# US/CA-OAH-SpecialEd — California OAH Special Education Decisions

Full text of Special Education due-process **Decisions and Orders** of the
California **Office of Administrative Hearings (OAH)**, Special Education
Division.

OAH is California's central, independent administrative tribunal. Its
Special Education Division hears due-process disputes between a
parent/student and a school district (local educational agency) under the
federal **IDEA** and the **California Education Code**. An Administrative
Law Judge issues a **Decision** that resolves each specific contested case
— i.e. **case_law**.

## Source

- Decision library (Sitecore media folder):
  `https://www.dgs.ca.gov/OAH/Case-Types/Special-Education/Services/Decisions`
- Enumeration (public MediaSearch API, HTML fragment, 10 results/page):
  `https://www.dgs.ca.gov/api/sitecore/MediaSearch/GetSearchResults?page={N}&folderPath=/sitecore/media library/Divisions/OAH/Special Education/SE Decisions&sortBy=date_desc`
  (send `X-Requested-With: XMLHttpRequest` + a `Referer`; the on-page
  `/sitecore/shell/...` prefix is 401-blocked for external clients, the
  `/api/sitecore/...` prefix is public)
- Documents: born-digital text-layer PDFs at `/-/media/{guid}.pdf`

~**1,925** decisions are indexed (pages 0..~192). Each PDF opens
`BEFORE THE OFFICE OF ADMINISTRATIVE HEARINGS STATE OF CALIFORNIA`, carries
the OAH case number(s) and the decision date in the body, and is extracted
via `common.pdf_extract` (no OCR needed). No CAPTCHA, no auth.

## Fields

`_id`, `_source`, `_type=case_law`, `case_number`, `case_numbers`,
`parties`, `issuer`, `title`, `text` (full decision), `url`, `date`,
`jurisdiction=US-CA`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~1,925 decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Notes

- Sibling OAH folders on the same Sitecore media host use the same
  MediaSearch mechanism and are easy extensions: General Jurisdiction
  decisions and the DDS (Lanterman Act) decisions folder.
- `date` prefers the first real "Month D, YYYY" in the decision body
  (typically the cover decision date), falling back to the listing
  Document Date.

## License

[Public Domain — US Government Work (California state administrative decisions)](https://www.law.cornell.edu/uscode/text/17/105) — Decisions of the California Office of Administrative Hearings are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
