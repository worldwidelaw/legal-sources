# TR/AnayasaMahkemesi - Turkish Constitutional Court

**Country:** Turkey (TR)
**Data Type:** Case Law
**Status:** Complete

## Overview

The Turkish Constitutional Court (Anayasa Mahkemesi) is the highest legal authority
for constitutional review in Turkey. Its **Kararlar Bilgi Bankası (KBB)** publishes
five decision categories, all covered by this scraper:

| Category (`kararTipi`) | Description | Count | Coverage |
|---|---|---|---|
| `NormDenetimi` | Norm review — constitutionality of laws, decrees, regulations | ~5,579 | 1962+ |
| `BireyselBasvuru` | Individual applications (constitutional complaints) | ~17,426 | 2012+ |
| `SiyasiParti` | Political party cases (dissolution, warnings, audits) | ~1,875 | 1963+ |
| `YuceDivan` | Supreme Criminal Tribunal (trials of high officials) | ~15 | 2007+ |
| `YasamaDokunulmazligi` | Parliamentary immunity decisions | ~40 | 1967+ |

**~24,935 decisions total.**

## Data Access

> **2026 site rebuild.** The old paginated HTML listing (`/Ara?page=N`) and detail
> pages (`/ND/{year}/{no}`, `/BB/{year}/{no}`) were removed. Both
> `kararlarbilgibankasi.anayasa.gov.tr` and `normkararlarbilgibankasi.anayasa.gov.tr`
> now serve a React SPA under a `/kbb/` path prefix, fronting one JSON backend.

### 1. Enumeration — `POST /api/core/public/search`

```json
{"kararTipi": "BireyselBasvuru", "page": 1, "size": 100,
 "sort": "kararTarihi", "order": "asc"}
```

Returns `{"total": int, "page": int, "data": [...]}`. Pagination is **1-based**
(`page: 0` is coerced to page 1); `size` up to 1000 is accepted. No auth.
Results carry **metadata only** — no decision body.

### 2. Full text — `GET /api/core/public/download-decision`

```
?id={uuid}&type=pdf&decType={2|3|4|5|6}
header: X-Captcha-Verified: {epoch_ms}:{hmac_sha256(epoch_ms, key)}
```

`decType`: BireyselBasvuru=2, NormDenetimi=3, SiyasiParti=4, YuceDivan=5,
YasamaDokunulmazligi=6. The HMAC key is a static string shipped in the SPA's
public JS bundle — a bot speed-bump, not authentication; the database is fully
open access. The response is a **born-digital PDF** that extracts cleanly with
PyMuPDF (no OCR needed).

`GET /api/core/public/merge-html/{uuid}` exists but returns only the *petition*
(dava dilekçesi) for `NormDenetimi`, not the decision, so it is not used.

### Turkish glyph repair

The court's Word→PDF pipeline substitutes G-cedilla / G-dotaccent glyphs for the
Turkish S-cedilla / I-dotaccent ones (`BaĢvuru` → `Başvuru`, `Ġdare` → `İdare`).
`bootstrap.py` repairs this case-aware during extraction.

## Fields Captured

| Field | Description |
|-------|-------------|
| `decision_id` | `{kararTipi}/{backend uuid}` |
| `database` | Decision category (`kararTipi`) |
| `title` | `E.{esas}, K.{karar}`, or applicant + `B. No:` for individual applications |
| `text` | **Full text of the decision** (extracted from PDF) |
| `date` | Decision date (ISO 8601) |
| `case_number` | Case reference (Esas Sayısı) |
| `decision_number` | Decision reference (Karar Sayısı) |
| `application_number` | Application reference (Başvuru Numarası) |
| `applicant` | Applicant name (individual applications) |
| `panel` | Deciding chamber (Karar Veren Birim) |
| `decision_type` | Outcome label (Esas - İptal, Esas (İhlal), …) |
| `summary` | Subject of the case (Karar Konusu), HTML stripped |
| `official_gazette_date` / `official_gazette_number` | Resmî Gazete publication |
| `pdf_url` | Direct PDF download URL |

## Usage

```bash
# Connectivity + extraction check across all five categories
python bootstrap.py test

# Sample mode (spread across all categories)
python bootstrap.py bootstrap --sample --sample-size 15

# Full bootstrap (resumable)
python bootstrap.py bootstrap
python bootstrap.py bootstrap-fast

# Incremental update
python bootstrap.py update
```

## Notes

- Language: Turkish, UTF-8.
- Requires **PyMuPDF** (`fitz`) for PDF text extraction.
- Rate limit: 2 req/s.
- **Resumable.** `data/checkpoint.json` records completed categories and the last
  finished page per category, so a killed or timed-out fleet run resumes instead
  of re-walking the corpus from 1962. Sample runs never advance the checkpoint.

## License

[Open Government Data](https://www.anayasa.gov.tr) — official decisions published by the Constitutional Court of the Republic of Turkey.
