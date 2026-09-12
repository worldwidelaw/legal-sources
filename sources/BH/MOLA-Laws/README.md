# BH/MOLA-Laws — Bahrain Ministry of Legal Affairs: Consolidated Laws

Consolidated laws and legislative decrees of the Kingdom of Bahrain, published
bilingually (English + Arabic) by the **Ministry of Legal Affairs (MoLA)**.

- **Listing:** https://www.mola.gov.bh/EN/Legislation/Laws/
- **Source type:** legislation
- **Coverage:** 66 consolidated laws & legislative decrees, full text
- **Languages:** English + Arabic (57 bilingual PDFs, 9 Arabic-only)
- **Auth:** none

## How it works

The listing is an ASP.NET Razor Pages app whose grid is filled by an XHR:

```
POST /EN/Legislation/Laws/?handler=GetLegislations
RequestVerificationToken: <__RequestVerificationToken from the listing page>
pageNo=1&pageSize=200&searchKeyWord=&sortOption=
-> {"items": [...], "totalRecords": 66}
```

Each item carries `lawNo`, `gazetteNo`, `date`, `statusLegalText`, an official
abstract, and `documentInEnglish` / `documentInArabic` PDF paths. The scraper
downloads each PDF and extracts the full consolidated text with PyMuPDF
(`fitz`). Each PDF contains the full law text plus inline notes of any
subsequent amendments.

Two site behaviours the scraper works around:

- **WAF on User-Agent.** The static assets and the XHR return HTTP 403 to short
  or absent `User-Agent` strings; a full browser UA plus `Referer` is required.
- **Decomposed lam-alef ligatures.** The PDFs draw `لا` as one ligature glyph
  whose `ToUnicode` maps to two code points in *visual* order, so plain
  `get_text()` yields ALEF+LAM and turns `إخلاء` into `إخالء` — visually close
  but unmatchable by keyword search. A genuine definite article `ال` is
  identical at the code-point level, so `_page_text()` discriminates on
  geometry: the ligature's alef is always zero-width (the following lam carries
  the whole advance) and only those pairs are swapped back.

## Usage

```bash
python bootstrap.py test-api             # Connectivity / parse check
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap            # Full pull (66 laws) -> data/records.jsonl
python bootstrap.py bootstrap-fast       # Alias for the full pull (VPS wrapper)
```

## Notes

This site previously returned HTTP 403 to the project (see issue #822) and was
marked blocked; as of 2026-06-11 it serves the listing and PDFs normally.
mola.gov.bh was then redesigned, replacing the server-rendered listing table
with the XHR above — the old table parser found 0 laws (issue #1602), which the
sample fallback masked. Rewritten 2026-09-09; the full pull now streams to
`data/records.jsonl` and exits 1 rather than reporting an empty success. Unlike
`BH/MIA-Gazette` (gazette *issues*), this source provides individual
**consolidated law texts** with official English translations, which are more
directly useful for legal retrieval.

## License

[Bahrain Government Open Data](https://www.data.gov.bh/en/ODPolicy) — official
government legislation published by the Ministry of Legal Affairs; open access,
commercial use permitted with attribution.
