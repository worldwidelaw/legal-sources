# US/MA-AGOpinions — Massachusetts Attorney General Opinions

The **official opinions of the Massachusetts Attorney General** — the AG's
authoritative (advisory) interpretations of Massachusetts law, issued to state
officers, boards, and the General Court (legislature).

> ⚠️ **Status: BLOCKED — `ocr_garbled_no_usable_fulltext`.** The scraper is
> complete and the corpus enumerates cleanly (181 items), but the DSpace OCR text
> is **de-spaced/garbled** (words run together, e.g. `BUSINESSANDLABORPROTECTIONBUREAU`)
> and the ORIGINAL PDFs are poorly-scanned, so the full text is near-useless for
> search/embedding and is **not ingested**. **Unblock = re-OCR the ORIGINAL scanned
> PDFs** with a good engine (tesseract / opendataloader-pdf) on an OCR-capable VPS;
> `bootstrap.py` already resolves the ORIGINAL PDF bitstream URLs.

## Coverage

- **173 annual reports**, 1832–2006 — "Report of the attorney general for the
  year ending …", the vehicle in which AG opinions were officially published.
- **8 bound volumes**, 1891–1929 — "Official opinions of the Attorney-General of
  the Commonwealth of Massachusetts" (Volumes 1–8), the multi-year opinion series.
- **181 items total** (180 with full text).

This is **doctrine** (advisory legal opinions), distinct from:
- `US/MA-Legislation` — the current consolidated General Laws.
- `US/MA-SessionLaws` — the as-enacted Acts & Resolves.

## Source / access

State Library of Massachusetts **DSpace 7 REST API** at
`https://archives.lib.state.ma.us/server/api` — open, no auth, no WAF (the same
repository as `US/MA-SessionLaws`).

- **Enumeration:** every AG-opinion item carries the LCSH subject
  `Attorneys general's opinions -- Massachusetts.`. A single Discover query
  (`/discover/search/objects?query=dc.subject.lcsh:"..."`) returns all 181 items,
  well under DSpace's deep-paging ceiling, so no year-partitioning is needed.
- **Full text:** `embed=bundles/bitstreams` returns each item's files inline; the
  full text is the DSpace-extracted plain-text bitstream in the `TEXT` bundle
  (recent reports are clean; older volumes are readable OCR of print). No local
  PDF/OCR step.

## Usage

```bash
python3 bootstrap.py test-api            # connectivity / extraction check
python3 bootstrap.py bootstrap --sample  # 12-record sample
python3 bootstrap.py bootstrap           # full pull (all opinions)
python3 bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public domain — US state government work (17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — no attribution required.

Opinions of the Massachusetts Attorney General are official government works in
the public domain (edicts/works of government; *Banks v. Manchester*). Digitized
and served openly by the State Library of Massachusetts. Commercial use OK.
