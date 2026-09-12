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

- Decision library (Sitecore MediaSearch widget):
  `https://www.dgs.ca.gov/OAH/Case-Types/General-Jurisdiction/Resources/DDS-Decisions`
- Pager (authoritative — the listing page itself renders only 10 rows):
  `https://www.dgs.ca.gov/api/sitecore/MediaSearch/GetSearchResults?page={N}&folderPath=/sitecore/media library/Divisions/OAH/General Jurisdiction/DDS Decisions&sortBy=date_desc`
- Documents: born-digital text-layer PDFs served by opaque GUID at
  `/-/media/<32-hex-guid>.pdf`

~**2,551** decisions are indexed (10 per page, pages 1..256,
newest→oldest). The human filename survives as the result-row link text
and is used as the `doc_id`; filenames begin with the 10-digit OAH case number
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

- Enumeration matches the sibling **US/CA-OAH-SpecialEd** source: the
  MediaSearch AJAX endpoint, called with `X-Requested-With:
  XMLHttpRequest` and a `Referer`. Before 2026-08 this folder was a plain
  server-side `?page=N` HTML listing linking to *named* PDFs under
  `/-/media/Divisions/OAH/General-Jurisdiction/DDS-Decisions/`; that
  scheme is gone and the old scraper discovered 0 documents (issue
  [#1397](https://github.com/ZachLaik/LegalDataHunter/issues/1397)).
- `date` prefers the **last** "Month D, YYYY" in the decision body
  (typically the cover/signature decision date; the first date is usually
  the hearing date), falling back to the MediaSearch row's
  "Document Date".

## License

[Public Domain — US Government Work (California state administrative decisions)](https://www.law.cornell.edu/uscode/text/17/105) — Decisions of the California Office of Administrative Hearings are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
