# US/CT-PURA — Connecticut Public Utilities Regulatory Authority (PURA) Final Decisions

Full text of **Final Decisions** issued by the Connecticut Public Utilities
Regulatory Authority (PURA, formerly the Department of Public Utility Control /
DPUC), adjudicating utility dockets across the electric, gas, water,
telecommunications, CATV, CBYD, restructuring and renewables industries. Each
Final Decision is an administrative adjudication of a specific docket
(**case_law**). ~24,000 decisions.

## Source

- **Landing page:** https://portal.ct.gov/pura/docket/final-decision-database
- **Backend:** public "Final Decision Database", a Lotus Domino app at
  `https://www.dpuc.state.ct.us/FINALDEC.NSF` (no auth).

## How it works

1. **Discovery.** Domino exposes the categorized view
   `UtilityByDecisionDateView` as machine-readable XML via `?ReadViewEntries`.
   A plain call lists the ~20 industry categories with their descendant counts;
   per-category pagination
   (`&RestrictToCategory={cat}&Start={n}&Count={c}`) walks every leaf document
   (Domino caps a single response at 1000 entries, so the scraper pages 200 at
   a time). Each leaf `<viewentry>` carries the document `unid` plus columns
   `AbbrevDckTitle` (title), `Decision_Date` (YYYYMMDD) and `DocketNumber`.
2. **Full text.** `normalize()` opens the Domino document
   (`/FINALDEC.NSF/0/{unid}?OpenDocument`) and downloads its born-digital
   decision attachment (`.../$FILE/{name}`).

   PURA publishes each decision in whichever format the Authority signed it
   in, so the attachment is not always a PDF. Measured over a 126-document
   sample spanning seven industry categories:

   | format | share | extractor |
   |---|---:|---|
   | `.pdf` | 55.6% | `fitz`/PyMuPDF, Tesseract OCR fallback for the rare image-only scan |
   | `.docx` | 24.6% | `common.doc_extract.extract_docx_text` (stdlib `zipfile`, no python-docx) |
   | `.doc` | 19.0% | `common.doc_extract.extract_doc_text` (Word 97-2003 piece table via `olefile`) |

   Matching only `.pdf` silently dropped the Word half of the corpus —
   roughly 10,800 of ~24,900 decisions, concentrated in the pre-2019 record.
   See issue #1262.

Discovery fails loud: `ReadViewEntries` raises `SourceBlockedError` rather
than returning an empty string, and the CLI exits non-zero on a block or a
0-record run, so a refused vantage can no longer be mistaken for an empty
database (which is what let the fleet ingest `sample/` as a false completion).

`fetch_all()` is checkpointed to `data/ct_pura_checkpoint.json` so fleet reruns
advance monotonically (completed categories are skipped; an in-progress
category resumes at its last `Start` offset; the loader dedups on `_id`).

## Usage

```bash
python bootstrap.py test-api            # connectivity + one full-text decision
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all Final Decisions)
python bootstrap.py bootstrap-fast      # high-throughput full pull (VPS)
```

## Record schema

`_id`, `_source` (`US/CT-PURA`), `_type` (`case_law`), `_fetched_at`, `docket`,
`title`, `text` (full decision text), `url`, `document_url`, `document_format`
(`pdf`/`docx`/`doc`), `date` (ISO 8601).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— Connecticut PURA Final Decisions are official state government edicts in the
public domain; commercial use permitted, no attribution required.
