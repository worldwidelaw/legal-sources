# BN/AGCLaws — Brunei Attorney General's Chambers Laws

**Source:** [Laws of Brunei — Texts of Acts](https://www.agc.gov.bn/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx)
**Data types:** legislation

## Coverage

~220 chapters of the consolidated Laws of Brunei Darussalam. Each chapter row on
the index links the principal Act plus any subsidiary rules and orders, so the
scraper yields ~312 documents. Full text comes from the born-digital PDFs; a few
1984-edition scans have no text layer and are skipped.

## Index fallback

Between the 2026-03-11 and 2026-05-14 Wayback captures the AGC stripped the
PDF-link column from the live index page — chapter numbers and titles remain, but
the links (and the B.L.R.O. revision column) are gone. The PDFs themselves are
still served. So `_parse_index()`:

1. Parses the live page; if any row still carries a PDF link it is used as-is.
2. Otherwise walks the Wayback CDX captures newest-first, taking the first one
   that still has the link column, and overlays the current titles read from the
   live page.

If AGC restores the links, step 1 takes over again with no code change.

## Usage

```bash
python bootstrap.py test-api             # index + one PDF extraction
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap --full     # full corpus to data/records.jsonl
python bootstrap.py bootstrap-fast       # same, concurrent PDF extraction
```

Full runs rely on `extract_pdf_markdown`'s skip-if-already-in-Neon check; sample
and test runs force re-extraction so they always produce records.

## License

[Open access — Brunei AGC legislation portal](https://www.agc.gov.bn/Pages/TermsofUse.aspx) — Government of Brunei Darussalam legislation published for public access; attribution to the Attorney General's Chambers.
