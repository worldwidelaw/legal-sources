# IN/AERB — Atomic Energy Regulatory Board (India)

Full text of the regulatory corpus published by the **Atomic Energy Regulatory
Board (AERB)**, the authority constituted under the Atomic Energy Act, 1962 to
regulate nuclear and radiation safety in India.

- **Site:** https://www.aerb.gov.in/
- **Read via:** Internet Archive (Wayback Machine) — see *Access* below
- **Volume:** ~2,080 unique documents (2,700+ archived PDF snapshots, deduplicated)
- **Types:** `doctrine` (safety codes, guides, standards, manuals, annual
  reports, guidance), `legislation` (Acts, Rules, GSR notifications AERB
  administers), `case_law` (licensing / consent decisions for named
  installations)

## Coverage

| Category | Examples |
|---|---|
| Safety Codes & Guides | `AERB/NPP-PHWR/SG/D-21`, `AERB/SG/IS-3` (personal protective equipment), `AERB/FE-FCF/SG-3` (uranium oxide fuel fabrication), `AERB/HWP/SG-2` (heavy water plants), regulatory-consents and safety-classification guides |
| Acts & Rules | Atomic Energy Act 1962 (+ 2015 amendment), Radiation Protection Rules 2004, Atomic Energy (Factories) Rules 1996, Atomic Energy (Safe Disposal of Radioactive Wastes) Rules 1987, Civil Liability for Nuclear Damage Act 2010 & Rules 2011, GSR notifications |
| Licensing decisions | Clearance for first approach to criticality, operating-licence grants / renewals / validity notes, site approvals, INES event ratings |
| Doctrine | Annual reports, Safety Research Institute highlights, glossaries, technical guidance, official statements |

Hindi translations of Acts and Rules are captured as separate records
(`language: "hi"`).

## Access

`aerb.gov.in` **resets the TLS handshake for every connection from non-Indian
vantages**: port 80 answers with a 302 to https, port 443 sends a TCP RST at
Client Hello. Verified 2026-08-06 with LibreSSL and OpenSSL 3.6 clients, a
browser User-Agent, TLS 1.2 pinning and a bare-IP/no-SNI request — all fail
identically, while DNS and ICMP are fine.

The corpus is therefore read from the **Internet Archive**, which holds a deep
crawl of the Board's document tree (most recent captures December 2025). The
scraper still tries the live host first and latches to archive-only after three
consecutive live failures with no live success, so the same code runs unchanged
from an Indian vantage.

## How it works

1. `fetch_all()` enumerates every archived `aerb.gov.in` `*.pdf` via the Wayback
   CDX API. Rows whose archived status is 4xx/5xx are dropped; **revisit rows
   (`statuscode "-"`) are kept** — a CDX `statuscode:200` filter hides them and
   would truncate the corpus by roughly a fifth.
2. AERB has reorganised its site several times, so the same document is reachable
   under several path schemes (`/AERBPortal/pages/English/t/publications/CODESGUIDES/`,
   `/T/PUBLICATIONS/CODESGUIDES/`, `/images/PDF/`, `/storage/uploads/documents/`,
   plus `index.php/english/` prefixed variants). Records are de-duplicated on
   `(language, lowercase filename)`, preferring the modern `/images/PDF/` |
   `/storage/` scheme and then the newest capture.
3. Directories are crawled round-robin with the regulatory core first (codes &
   guides, acts & rules, then documents, press releases, everything else, forms
   last), so a truncated run still spans the corpus.
4. `normalize()` downloads the raw archived PDF (`/web/{ts}id_/{url}`) and
   extracts full text with the shared `common.pdf_extract` helper (PyMuPDF, with
   pdfplumber and OCR fallbacks). Titles come from the first substantive line of
   the PDF (masthead boilerplate skipped, wrapped all-caps titles rejoined);
   dates from the filename, falling back to the earliest archive capture date.
   Recruitment/tender PDFs and documents under 400 characters of text (blank
   application forms, cover sheets) are dropped.

## Usage

```bash
python bootstrap.py test-api             # Connectivity / enumeration test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## Known limitations

- A minority of older PDFs are scanned images; they need OCR (Tesseract /
  `opendataloader-pdf`) and are skipped when it is unavailable.
- Documents AERB published after the last Internet Archive capture are only
  reachable from an Indian vantage.

## License

[Government Open Data License — India (GODL-India)](https://data.gov.in/government-open-data-license-india) — attribution required; commercial use permitted.

AERB is a Government of India regulatory body; its published safety codes,
guides, acts, rules and press material fall under the standard GoI copyright
policy / GODL-India, which permits reproduction — including for commercial
purposes — provided the material is reproduced accurately, attributed and not
used in a misleading context. Third-party copyright material embedded in AERB
documents is excluded.
