# UK/EducationTribunalWales — Education Tribunal for Wales (Tribiwnlys Addysg Cymru)

Full-text **decisions** of the **Education Tribunal for Wales (ETW)**, the
independent statutory tribunal for Wales that determines:

- **Additional Learning Needs (ALN) appeals** under the Additional Learning Needs
  and Education Tribunal (Wales) Act 2018 (and legacy **Special Educational Needs
  (SEN) appeals** under the Education Act 1996), and
- **disability discrimination claims** in the field of education under the
  Equality Act 2010.

ETW (formerly the Special Educational Needs Tribunal for Wales, SENTW) issues
written, reasoned decisions that are binding on the parties (local authority,
governing body and the child/parent) and appealable on a point of law to the
Upper Tribunal. Decisions are published **anonymised**.

This is **GB-WLS (Wales)** case law that is **not** on the National Archives
*Find Case Law* service (which indexes England & Wales superior courts and
reserved UK tribunals), so it is not covered by `UK/CaseLaw`.

## Source

- **Site:** https://educationtribunal.gov.wales (bilingual English/Cymraeg,
  Drupal)
- **Decisions:** https://educationtribunal.gov.wales/decisions
- **Auth:** none (free public access)
- **Coverage:** September 2020–present (~50 decisions and growing)

## How it works

The site publishes decisions by September–September "school year" window:

1. `/previous-decisions` lists the school-year window links
   (`/decisions/3/{YYYY-09--YYYY-09}`).
2. Each window lists per-decision detail-page slugs, e.g.
   `/additional-learning-needs-appeal-decision-07`,
   `/disability-discrimination-claim-decision-02-2`,
   `/special-educational-needs-appeal-decision-01`.
3. Each detail page carries the title and one **born-digital** decision PDF under
   `/sites/educationtribunal/files/`.

The scraper downloads each PDF and extracts full text with PyMuPDF (with a shared
pdfplumber/pypdf fallback). No OCR is needed. Decisions are anonymised (only years
survive in the body), so the `date` is set to the start of the school-year window
(`YYYY-09-01`); the exact school year is retained in `school_year`.

## Usage

```bash
python bootstrap.py test               # connectivity + one extraction
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (recent windows)
```

## Record shape

Each normalized record contains `_id`, `_source` (`UK/EducationTribunalWales`),
`_type` (`case_law`), `title`, `text` (full decision text), `date`, `url`,
`case_ref`, `series` (ALN appeal / SEN appeal / disability discrimination claim),
`school_year`, `pdf_url`, `court`, `jurisdiction` (`GB-WLS`) and `language`.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — Crown copyright; free to use and re-use in any format under the OGL (attribution required, must reproduce accurately and not in a misleading context). Commercial use permitted.
