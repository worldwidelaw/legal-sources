# INTL/ASEAN-Legal — ASEAN Legal Instruments Database

**Source:** [https://agreement.asean.org/](https://agreement.asean.org/)
**Data types:** legislation

## Access

Live-first, Internet-Archive fallback. Since 2026-08 `agreement.asean.org` answers
every request (listing, detail pages, PDFs) with a Sucuri CloudProxy JavaScript
challenge (HTTP 307 + JS-eval body), so the scraper reconstructs the corpus from
the Wayback Machine:

1. CDX-enumerate `agreement.asean.org/agreement/detail/{id}.html` captures and keep
   the newest HTTP 200 per document id (~256 instruments).
2. Parse the archived detail page for title, pillar, place/date of signature,
   ratification, status and the `Document/External Information Source` PDF link.
3. Replay `/media/download/{stamp}.pdf` through `/web/{ts}id_/` and extract full text.

The live path is retried first on every run and takes over automatically once the
challenge is lifted. Enumerating zero documents from both paths now raises instead
of reporting a silent success. A minority of the oldest instruments are scanned
images and are skipped unless OCR is available.

## License

Published by the ASEAN Secretariat. ASEAN agreements, protocols, and conventions are public international law instruments. Copyright ASEAN Secretariat; no formal open data license published.
