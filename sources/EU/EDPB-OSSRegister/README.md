# EU/EDPB-OSSRegister — EDPB Article 60 One-Stop-Shop Register

Final decisions by EU national data protection authorities (DPAs) under the
GDPR cross-border cooperation mechanism (Article 60). Published by the
European Data Protection Board (EDPB).

## Data source

- **Register**: <https://www.edpb.europa.eu/registers/register-of-final-one-stop-shop-decisions_en>
  — server-rendered Drupal view, `?page=N`, 11 `div.foss-decision-teaser` rows per page
  (~122 pages)
- **Full text**: each teaser links one or more decision PDFs under `/system/files/…`,
  downloaded and extracted with PyMuPDF (pdfplumber / `common.pdf_extract` fallback)
- **Records**: 1,341 decisions as of 2026-08-20 (the view reports its own total, which
  the fetcher logs as a coverage check)
- **Coverage**: all EU/EEA DPAs, identified by EDPBI identifier (`EDPBI:{SA}:OSS:D:{year}:{n}`)
- **Fields**: case ID, decision date, lead SA, concerned SAs, main legal reference,
  relevant topics, outcome, PDF URL

## Known gaps

A minority of the register's PDFs are scanned images with no text layer (Luxembourg's
recent decisions in particular). Those records are skipped rather than ingested empty;
OCR is not run. Everything else is born-digital and extracts cleanly — samples run
2.6K–38K chars.

## Usage

```bash
python3 bootstrap.py bootstrap --sample     # 15 records into sample/
python3 bootstrap.py bootstrap-fast --full  # full corpus into data/records.jsonl
python3 bootstrap.py updates --since 2026-01-01
```

## Fetch notes

The EDPB CDN intermittently ends a chunked PDF stream early (`Response ended
prematurely`). The fetcher requests identity encoding and resumes truncated downloads
with HTTP `Range` requests rather than restarting, so large born-digital decisions are
not silently dropped.

The register moved in 2026 from
`/our-work-tools/consistency-findings/register-for-article-60-final-decisions_en`
(now a 301) and its markup was rebuilt; the old
`div.node--type-edpb-article-60-final-decision` selectors matched nothing. Enumeration
now fails loud when page 0 yields no rows instead of emitting an empty corpus.

## License

[EUR-Lex Legal Notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — reuse permitted with attribution.
