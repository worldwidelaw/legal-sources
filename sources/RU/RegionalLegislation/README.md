# RU/RegionalLegislation — Regional Legislation of the Subjects of the Russian Federation

Officially-published legal acts of every subject of the Russian Federation
(republics, krais, oblasts, autonomous okrugs, federal cities), from the
**Official Internet Portal of Legal Information** (`publication.pravo.gov.ru`),
`subjects` block.

This is the sub-national ("oblast-level") companion to **RU/PravoGovRu**, which
covers only *federal* laws. Together they give federal + regional coverage of
Russian legislation. Fulfils issue #1076 (split Russia into oblast-level
subnational coverage).

## Scope

- Regional laws (законы субъекта РФ)
- Decrees / ordinances of the head of the region (указы / постановления главы)
- Resolutions and orders of regional governments (постановления / распоряжения
  правительства субъекта)
- Acts of regional ministries and agencies

~1.5M documents (`itemsTotalCount` from the API), all `legislation`.

## Access (no auth, no CAPTCHA, no JavaScript)

Public JSON API + signed-PDF full text:

1. **Enumerate** (newest first), 200/page — valid `PageSize` ∈ {10, 30, 100, 200}:
   ```
   GET /api/Documents?block=subjects&PageSize=200&Index={N}
   -> { items: [{ eoNumber, complexName, name, number, documentDate,
                  signatoryAuthorityId, publishDateShort, ... }],
        itemsTotalCount, pagesTotalCount }
   ```
2. **Issuing authority** (region body) names:
   ```
   GET /api/SignatoryAuthorities?blockCode=subjects  ->  [{ id, name }, ...]
   ```
3. **Full text** — signed act PDF:
   ```
   GET /file/pdf?eoNumber={eoNumber}
   ```
   These are scanned/signed images with no text layer. PyMuPDF cannot composite
   the 1-bit embedded image when rendering the page (it renders blank), so the
   scraper extracts the **embedded image object** of each page directly and OCRs
   it with tesseract (`lang=rus`). The rare born-digital act with a real text
   layer is used as-is.

## Sub-jurisdictions

Omni-source covering all federal subjects; tagged in `manifest.yaml` with the
`RU-*` wildcard. The API supports finer per-region splits via
`signatoryAuthorityId` should oblast-by-oblast sources be wanted later.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples (newest first)
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

Requires `PyMuPDF` (fitz), `Pillow`, `pytesseract` and the `tesseract` binary
with the Russian language pack (`rus`).

## License

[Russian official document — public domain](http://publication.pravo.gov.ru/) —
official documents of state bodies (including of the subjects of the Russian
Federation) are not objects of copyright under **art. 1259(6) of the Civil Code
of the Russian Federation**. No attribution required; commercial use permitted.
