# US/NV-EthicsOpinions — Nevada Commission on Ethics Opinions & Determinations

Full-text opinions and determinations published by the **Nevada Commission on
Ethics** under the Nevada Ethics in Government Law (**NRS Chapter 281A**).

The Commission publishes two classes of documents, distinguished by the suffix of
the case number:

- **Advisory Opinions** (suffix `A`, e.g. `24-105A`) — the Commission's written
  interpretations, requested by a public officer or employee, construing the
  conflict-of-interest, gift, disclosure and use-of-position provisions of NRS
  281A. Classified as **doctrine**.
- **Adjudicatory documents** (suffix `C`, e.g. `25-024C`) — Panel Determinations,
  Stipulated Agreements, Deferral Agreements and Settlement Agreements resolving a
  specific ethics complaint against a named public officer/employee. Classified as
  **case_law**.

## Source

- Opinions page: https://www.ethics.nv.gov/opinions/
- Each entry is a direct born-digital PDF link whose anchor text carries the case
  caption and case number.

## Method

1. `GET /opinions/` and parse every PDF `<a>` anchor (case number + caption).
2. Derive document type from the case-number suffix (`A` → doctrine, `C` → case_law).
3. Download each PDF (`curl -L`, following the `/uploadedFiles` → `/siteassets`
   301 redirects) and extract its text layer via the shared `common.pdf_extract`
   backend. No OCR is required — the PDFs are born-digital.
4. Parse the decision date from the PDF filename, falling back to the two-digit
   year in the case number.

## Coverage

This covers the **recent-opinions corpus** surfaced on the Commission's Opinions
page (~35 documents). The complete historical archive is served by the PDI Online
portal (`nvethics.pdi.online/public/dbo_AdvisoryOpinion`), an ASP.NET application
whose list/search XHR endpoint is browser-bound — a future extension.

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # Full pull
```

## License

[Public Domain (State of Nevada Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
opinions and determinations of the Nevada Commission on Ethics are official public
records of the State of Nevada, published for public use with no copyright
restriction. Commercial use permitted.
