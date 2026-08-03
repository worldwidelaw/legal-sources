# IAEA Information Circulars (INFCIRC) — International Nuclear Law

**Source:** [https://www.iaea.org/publications/documents/infcircs](https://www.iaea.org/publications/documents/infcircs)
**Country:** INTL
**Data types:** legislation
**Status:** Complete

## What this source contains

The **INFCIRC** (Information Circular) series is the International Atomic Energy
Agency's public collection of official "INFORMATION CIRCULAR" documents that carry
the authoritative texts of nuclear-law instruments, including:

- **Safeguards agreements** — e.g. INFCIRC/153 (the comprehensive safeguards
  agreement model) and INFCIRC/540 (the Model Additional Protocol), plus the
  member-state-specific agreements and additional protocols.
- **Texts of international conventions** — physical protection (INFCIRC/274),
  early notification of a nuclear accident (INFCIRC/335), assistance in a nuclear
  emergency (INFCIRC/336), nuclear safety, and the joint convention.
- **Export-control guidelines** — INFCIRC/254 (the Nuclear Suppliers Group
  Guidelines) and its revisions.
- **Agency statutes / relationship agreements** and member-state communications
  (notes verbales circulated for the information of all Member States).

~1,480 English-language documents from 1959 to the present.

## Access method

The live host `www.iaea.org` sits behind Cloudflare and returns **HTTP 403** to
datacenter / foreign vantages for both the listing and the
`/sites/default/files/publications/documents/infcircs/{year}/infcirc{n}.pdf` PDFs
(see issue #1093). The **Internet Archive** has preserved the full INFCIRC PDF
tree at that exact path. The scraper therefore:

1. Enumerates every archived `infcirc{n}.pdf` via the **Wayback CDX API**
   (English only — `_fr/_ru/_es/_ar/_zh/...` language variants are filtered out).
2. Fetches the raw PDF bytes through the `/web/{timestamp}id_/{url}` replay
   endpoint (bypasses the Cloudflare block).
3. Extracts the full text with PyMuPDF (born-digital documents; a small number of
   1959-era image scans with no text layer are skipped and logged).

The canonical `iaea.org` URL is retained as each record's `url`. A checkpoint file
(`data/processed.json`) makes runs resumable and gentle on the Internet Archive.

## License

[IAEA official document / IGO edict](https://www.iaea.org/about/terms-of-use) — IAEA
Information Circulars are official documents of an intergovernmental organization,
published for public distribution ("Distr. GENERAL" / "General Distribution").
Treated as public government/IGO edicts; attribution to the IAEA appreciated.
