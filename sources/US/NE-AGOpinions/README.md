# US/NE-AGOpinions — Nebraska Attorney General Opinions

Full text of official written opinions issued by the **Nebraska Attorney
General** under Neb. Rev. Stat. § 84-205. Each opinion is the AG's
authoritative interpretation of Nebraska law, issued at the request of the
Legislature, a state officer or a county attorney. Classified as
**doctrine** (official state legal interpretation / government edict).

## Source

- **Publisher:** Nebraska Attorney General
- **Site:** https://ago.nebraska.gov/opinions
- **Archive (all opinions):** https://ago.nebraska.gov/opinions/archive
- **Coverage:** 1990–present (~1,190 opinions)
- **Auth:** none

## How it works

1. `/opinions/archive` is a single server-rendered HTML table listing every
   opinion with its number, issue date, subject title, detail-page link and a
   born-digital PDF link at `/sites/default/files/docs/opinions/{file}.pdf`.
2. `fetch_all()` parses that table and yields one raw metadata dict per
   opinion (newest first).
3. `normalize()` downloads the PDF and extracts the full text with PyMuPDF
   (`fitz`). Almost every PDF carries a text layer; the rare image-only scan
   is OCR'd with Tesseract. Under `bootstrap-fast` the downloads overlap
   across worker threads.

Opinion numbers are canonicalised to `YY-NNN` — the older five-digit display
form (`90003`) and the modern hyphenated form (`26-003`) both map to the same
value.

## Usage

```bash
python bootstrap.py test-api             # connectivity + one-opinion text check
python bootstrap.py bootstrap --sample   # ~12 sample records to sample/
python bootstrap.py bootstrap-fast       # full pull (VPS)
```

## License

[Public Domain — 17 U.S.C. § 105 / government edicts doctrine](https://www.law.cornell.edu/uscode/text/17/105) — Nebraska Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
