# SO/OSAG — Somalia Official Gazette (OSAG)

Official Gazette (Faafinta Rasmiga Ah) of the Federal Republic of Somalia, published by
the Office of the State Attorney General (Xafiiska Garyaqaanka Guud ee Dawladda).

## Data

- **Type:** legislation (laws, cabinet decisions, resolutions, agreements)
- **Format:** scanned PDF (text recovered by OCR — see below)
- **Language:** Somali (so)
- **Coverage:** 2014–present, ~280 individual documents across 28 pages
- **Records:** 15 sample records with full text

## Text extraction

Every bulletin is a scan: page 1 is a born-digital masthead and each body page
is a single full-page bitmap. Some PDFs carry a text layer the publisher's
scanner produced by running an **English** OCR model over Somali; the rest
carry none at all. Reading those layers directly was the defect behind issue
#1411 — it yielded `Qodcbbada` for *Qodobbada*, `Gudcliga` for *Guddiga* and
article numbers read as `zo'"` (20aad), and for the no-text-layer PDFs it
returned only the ~790-char masthead while the law itself went missing.

The scraper therefore rasterizes each scanned page at 300 DPI and runs
tesseract itself, keeping the real text layer only for born-digital pages.
Tesseract ships no Somali model, but Somali is plain ASCII Latin and `eng`
reads it cleanly; `swa`, the nearest African Latin-script model, was markedly
worse (it substituted `z` for `x` throughout). 400 DPI bought no accuracy over
300, so 300 is used.

Two known residues, both upstream-scan artifacts rather than parser gaps:

- The ordinal suffix in `Qodobka <N>aad` is typeset as superscript and OCRs as
  punctuation noise. **Article headings are repaired** (`Qodobka 20"` →
  `Qodobka 20aad`), which is what unit segmentation cuts on. Inline *citations*
  to other laws (`Qodobka 87244`) are left as read — the digits and the suffix
  fuse there, and re-splitting them would mean inventing a number.
- Documents whose scanned pages yield under 400 characters are skipped and
  logged rather than stored as a masthead-only record.

Requires the `tesseract` binary plus `pytesseract`, `Pillow` and `PyMuPDF`.
The scraper raises rather than falling back when tesseract is missing —
silently reverting to the upstream layer is the bug this replaced.

Tuning is via environment variables: `OSAG_OCR_LANG` (default `eng`),
`OSAG_OCR_DPI` (300), `OSAG_OCR_MAX_PAGES` (400), `OSAG_OCR_PAGE_TIMEOUT`
(120s), `OSAG_OCR_DOC_TIMEOUT` (1800s).

## Categories

- Legislation (Sharci)
- Cabinet Decisions (Xeer)
- Agreements
- Resolutions
- Budget documents

## License

[OSAG Somalia](https://osagsomalia.com/) — Official government gazette, public domain under Somali law.
