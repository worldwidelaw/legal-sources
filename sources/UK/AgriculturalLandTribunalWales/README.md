# UK/AgriculturalLandTribunalWales — Agricultural Land Tribunal for Wales (Tribiwnlys Tir Amaethyddol Cymru)

Full-text decisions of the **Agricultural Land Tribunal for Wales** (ALT Wales),
the independent statutory tribunal for Wales (GB-WLS) that determines
agricultural-land disputes under the Agricultural Holdings Act 1986, the
Agriculture (Miscellaneous Provisions) Act 1976 and the Land Drainage Act 1991:
succession to agricultural tenancies, consents to the operation of notices to
quit, certificates of bad husbandry, and land-drainage / ditch and watercourse
disputes.

Its written, reasoned decisions are adjudicative **case law** for Wales. ALT
Wales is a **devolved Welsh tribunal** and is **not** on the National Archives
*Find Case Law* service (which covers England & Wales superior courts and
reserved UK tribunals only), so it is not captured by `UK/CaseLaw`. The tribunal
is bilingual — decisions may be issued in Welsh, English, or both.

- **Jurisdiction:** GB-WLS (Wales)
- **Type:** `case_law`
- **Coverage:** ~22 decisions, 2002–present (growing)
- **Auth:** none (free public access)

## How it works

`agriculturallandtribunal.gov.wales` is a Drupal site (same platform as
`UK/RPTWales`, `UK/WelshLanguageTribunal`, `UK/EducationTribunalWales`,
`UK/AdjudicationPanelWales`). Decisions are browsed by case type:

1. Three case-type pages list April–April "tribunal year" windows:
   `/land-drainage-applications` (window-id 3), `/tenancy-applications` (4),
   `/agricultural-applications-decisions` (5).
2. `/decisions/{3|4|5}/{YYYY-04--YYYY-04}` lists per-decision detail-page slugs
   `/alt-{NNNN}-{party-holding}`.
3. Each detail page carries the title (case ref `ALT NNNN`) and one or more
   decision PDFs under `/sites/agriculturalland/files/`.

Text extraction: newer decisions are **born-digital PDFs** (PyMuPDF). The older
2002–2019 decisions were bulk-scanned in 2020 (image-only, no text layer), so
the scraper falls back to **OCR** (tesseract, `eng+cym`) rendering each page to
PNG. Decision date = the tribunal-year window start (`YYYY-04-01`); decisions
cite many historical/statutory dates so the in-body date is unreliable, and the
PDF upload folder (`/files/YYYY-MM/`) is the upload month, not the decision date.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; free to use and re-use in any format under the OGL (attribution required, must reproduce accurately and not in a misleading context). Commercial use permitted.
