# US/AAO — USCIS Administrative Appeals Office Non-Precedent Decisions

**Source:** [https://www.uscis.gov/administrative-appeals/aao-decisions/aao-non-precedent-decisions](https://www.uscis.gov/administrative-appeals/aao-decisions/aao-non-precedent-decisions)
**Data types:** case_law

The Administrative Appeals Office (AAO) adjudicates administrative appeals of
denied USCIS immigration benefit requests (I-140 employment-based petitions,
T/U nonimmigrant status, refugee travel documents, waivers, etc.). It issues
tens of thousands of non-precedent decisions, organized by benefit-type code
and year of issuance.

## Approach

- The non-precedent listing is a Drupal view paginated with `?page=N`
  (0-indexed, 10 decisions/page, newest first). Iterate pages until the corpus
  is exhausted.
- Each entry anchor reads `"{Category} - {MON}{DD}{YYYY}_{seq}{code} (PDF, {size})"`
  with an href under `/sites/default/files/err/{CODE} - {Category}/Decisions_Issued_in_{YEAR}/`.
  The category, benefit code (from the `/err/{CODE}` folder), decision date
  (from the filename), and a redacted `In Re:` identifier (from the PDF body)
  are recovered.
- Full text extracted from born-digital PDFs via `common.pdf_extract` (text
  layer; no OCR needed).

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — US federal government works.
