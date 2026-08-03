# US/CA-OAH-GeneralJurisdiction — California OAH General Jurisdiction Decisions

Full text of **General Jurisdiction Decisions** of the California **Office
of Administrative Hearings (OAH)**, General Jurisdiction Division.

OAH is California's central, independent administrative tribunal. Its
General Jurisdiction Division adjudicates contested cases for ~1,500 state
and local agencies under the **Administrative Procedure Act** (Gov. Code
§ 11500 et seq.) — professional-license discipline, agency enforcement,
public-benefit and other administrative disputes. An Administrative Law
Judge issues a **Decision** that resolves each specific contested case —
i.e. **case_law**.

Distinct from the sibling **US/CA-OAH-SpecialEd** (special-education
due-process decisions), which uses the same MediaSearch host and mechanism
on a different folder.

## Source

- Decision library (Sitecore media folder):
  `https://www.dgs.ca.gov/OAH/Case-Types/General-Jurisdiction/Services/Decisions`
- Enumeration (public MediaSearch API, HTML fragment, 10 results/page):
  `https://www.dgs.ca.gov/api/sitecore/MediaSearch/GetSearchResults?page={N}&folderPath=/sitecore/media library/Divisions/OAH/General Jurisdiction/GJ Decisions&sortBy=date_desc`
  (send `X-Requested-With: XMLHttpRequest` + a `Referer`; the on-page
  `/sitecore/shell/...` prefix is 401-blocked for external clients, the
  `/api/sitecore/...` prefix is public)
- Documents: born-digital text-layer PDFs at `/-/media/{guid}.pdf`

~**5,344** media items are indexed. The folder mixes real decisions with
multilingual **"Notice of Collection"** privacy forms and **"Quarterly Data
Report"** statistics (no case number), plus **Accessibility-Modified/"084"**
remediated duplicate copies of each decision. Discovery therefore keeps
**only rows bearing a numeric OAH case number** and **dedupes by case
number** (one Decision per case). Each surviving PDF opens
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
python bootstrap.py bootstrap           # full pull (thousands of decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Notes

- The DDS (Lanterman Act) decisions folder on the same Sitecore media host
  uses the same MediaSearch mechanism and is a further easy extension.
- `date` prefers the first real "Month D, YYYY" in the decision body
  (typically the cover decision date), falling back to the listing
  Document Date.

## License

[Public Domain — US Government Work (California state administrative decisions)](https://www.law.cornell.edu/uscode/text/17/105) — Decisions of the California Office of Administrative Hearings are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
