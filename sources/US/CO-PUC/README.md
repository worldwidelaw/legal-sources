# US/CO-PUC — Colorado Public Utilities Commission Decisions

Full text of **Decisions** issued by the Colorado Public Utilities Commission
(PUC), the state regulator adjudicating rate cases, tariff filings,
certificates of public convenience and necessity (CPCN), complaints and
rulemakings across the electric, gas, water and transportation industries.
Each Decision is the Commission's administrative adjudication of a specific
proceeding — **case_law** (public domain, US state government edict).

## Source

- **Agency site:** https://puc.colorado.gov/puc-decisions
- **Decision system:** DORA E-Filings (EFI), `https://www.dora.state.co.us/pls/efi`
  (an Oracle PL/SQL web app)
- **Coverage:** 2000 – present (~800–900 decisions/year, ~20K total)

## How it works

1. The decision results endpoint `EFI_SEARCH_UI.getDecisionResults` is a plain
   GET. Although the interactive search form carries a `p_session_id` hidden
   field, the results endpoint works with an **empty** `p_session_id`: a GET
   with `p_after=MM/DD/YYYY` & `p_before=MM/DD/YYYY` (plus empty filter params)
   returns a server-rendered `<table>` of every decision issued in that window —
   decision number, a `Show_Decision?p_dec={id}` link + title, the issued date
   and the proceeding number. No authentication, cookie or token required.
2. `fetch_all()` walks years newest-first back to the EFI floor year (2000).
3. `normalize()` fetches the `Show_Decision` detail page, resolves the primary
   Decision PDF via `efi_p2_v2_demo.show_document?p_dms_document_id={id}`,
   downloads it and extracts the full text (fitz/PyMuPDF; Tesseract OCR fallback
   for the rare image-only scan).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + full-text check
python bootstrap.py bootstrap --sample   # ~12 sample decisions
python bootstrap.py bootstrap            # Full pull (all decisions)
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — Colorado)](https://www.law.cornell.edu/uscode/text/17/105) — Colorado PUC Decisions are official state government edicts in the public domain; no attribution required, commercial use permitted.
