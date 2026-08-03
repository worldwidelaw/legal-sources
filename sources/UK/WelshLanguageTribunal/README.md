# UK/WelshLanguageTribunal — Welsh Language Tribunal (Tribiwnlys y Gymraeg)

Full-text **decisions** of the **Welsh Language Tribunal (WLT, Tribiwnlys y
Gymraeg)**, the independent statutory tribunal for Wales established under the
**Welsh Language (Wales) Measure 2011**. It determines appeals and applications
against decisions of the **Welsh Language Commissioner** concerning
Welsh-language standards imposed on public bodies and other organisations.

Its written, reasoned decisions are binding on the parties and appealable on a
point of law to the Upper Tribunal. Decisions may be issued in **Welsh, English,
or both** (bilingual tribunal).

This is **GB-WLS (Wales)** case law that is **not** on the National Archives
*Find Case Law* service (which indexes England & Wales superior courts and
reserved UK tribunals), so it is not covered by `UK/CaseLaw`.

## Source

- **Site:** https://welshlanguagetribunal.gov.wales (bilingual English/Cymraeg,
  Drupal)
- **Decisions:** https://welshlanguagetribunal.gov.wales/decisions
- **Auth:** none (free public access)
- **Coverage:** April 2017–present (~25 decisions and growing)

## How it works

Decisions are published by April–April "tribunal year" window:

1. `/previous-decisions` lists the window links
   (`/decisions/4/{YYYY-04--YYYY-04}`).
2. Each window lists per-decision detail-page slugs, e.g.
   `/tygwlt165-pembrokeshire-county-council`,
   `/tygwlt2501-aled-robert-thomas`.
3. Each detail page carries the title (case ref `TYG/WLT/YY/NN`) and one or more
   **born-digital** decision PDFs under `/sites/welshlanguage/files/`.

The scraper downloads each PDF and extracts full text with PyMuPDF (with a shared
pdfplumber/pypdf fallback). No OCR is needed. The `date` is taken from a `YYMMDD`
prefix in the PDF filename when present, else the last date in the body text,
else the tribunal-year window start (`YYYY-04-01`). Language is auto-detected
(`cy`, `en`, or `cy,en`).

## Usage

```bash
python bootstrap.py test               # connectivity + one extraction
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (recent windows)
```

## Record shape

Each normalized record contains `_id`, `_source` (`UK/WelshLanguageTribunal`),
`_type` (`case_law`), `title`, `text` (full decision text), `date`, `url`,
`case_ref` (`TYG/WLT/YY/NN`), `pdf_url`, `court`, `jurisdiction` (`GB-WLS`) and
`language`.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; free to use and re-use in any format under the OGL (attribution required, must reproduce accurately and not in a misleading context). Commercial use permitted.
