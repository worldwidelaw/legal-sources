# US/OCAHO — Office of the Chief Administrative Hearing Officer Decisions

**Source:** [https://www.justice.gov/eoir/office-of-the-chief-administrative-hearing-officer-decisions](https://www.justice.gov/eoir/office-of-the-chief-administrative-hearing-officer-decisions)
**Data types:** case_law

OCAHO is the adjudicatory body within the Executive Office for Immigration
Review (EOIR) whose Administrative Law Judges decide contested cases under the
Immigration and Nationality Act's employer-sanctions (8 U.S.C. § 1324a),
document-fraud (§ 1324c) and unfair immigration-related employment-practice
(§ 1324b) provisions. Published decisions appear in the bound "OCAHO"
reporter, volumes 1–22+ (1988–present).

## Approach

- Harvest every volume-listing link from the OCAHO decisions index page. Two
  layouts coexist: volumes 1–11 (bound/looseleaf HTML lists with PDFs under the
  same `Volume{N}/` folder) and volumes 12+ (modern `listing-volume-{N}-decisions`
  pages with PDFs at `/d9/YYYY-MM/{ID}.pdf` or `/media/{N}/dl?inline`).
- Each listing is a table of `[caption, case number, date, "{ID} (PDF)"]` rows.
  A single row-based parser handles both layouts; continuation rows sharing a
  PDF are de-duplicated by PDF URL.
- Full text extracted from born-digital PDFs via `common.pdf_extract` (no OCR
  needed).

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — US federal government works.
