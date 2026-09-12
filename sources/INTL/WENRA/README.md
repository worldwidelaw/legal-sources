# INTL/WENRA — Western European Nuclear Regulators Association

Full text of the doctrine published by **WENRA**, the association of the heads of
the nuclear safety regulatory authorities of European countries with nuclear
power plants (plus associated members). WENRA writes the **Safety Reference
Levels** that national regulators then transpose into their own binding
frameworks, so its output is the de-facto harmonised European nuclear-safety
rulebook.

- **Site:** https://wenra.eu/publications
- **Volume:** ~220 documents (89 publication PDFs, ~100 news statements, 28
  recovered from the Internet Archive)
- **Type:** `doctrine`

## Coverage

| Category | Examples |
|---|---|
| Safety Reference Levels | SRLs for existing reactors, research reactors, waste storage, disposal and decommissioning, and their revisions (2007, 2008, 2014, 2020) |
| Implementation reporting | *Status of the implementation of the SRLs in national regulatory frameworks* reports, country by country, through 1 January 2026 |
| Working-group reports | RHWG (Reactor Harmonisation) — safety objectives for new NPPs, design extension conditions, practical elimination, fuel licensing, benchmarking; WGWD (Waste and Decommissioning); WGRR (Research Reactors); WGRSM (Radiation Sources and Materials); WIG (Inspection) |
| Hazard guidelines | Guidelines on the evaluation of seismic hazard, external flooding and accidental aircraft crash for new Class I nuclear installations |
| Positions & statements | WENRA recommendations, ENSREG-WENRA / ENSRA-WENRA / HERCA-WENRA joint statements, the series of positions on the safety of Ukrainian nuclear installations (ZNPP shelling, loss of off-site power, Chernobyl, Kakhovka), statements on SMRs, nuclear propulsion for merchant ships and periodic safety review |
| Institutional | Terms of reference, strategy documents, Topical Peer Review specifications |

## How it works

Three enumeration streams, de-duplicated on the decoded PDF filename stem and
round-robined so a truncated run — or the sample — spans all three:

1. **Publications view** (`/publications`). A Drupal 9 view that lists every
   published document on a single page (no pager) with its title, the issuing
   working group and the publication timestamp; each row links a born-digital
   PDF under `/sites/default/files/`. The thematic pages (`/ukraine`,
   `/workinggroups`, `/about`) are swept for the few PDFs the view omits.
2. **News nodes** (`/news-archive` → `/node/{id}`). The node body carries the
   full text of the announcement or position, read from the `node__content`
   container. A news node that links a PDF already collected from the
   publications view is skipped, so a statement published both ways is stored
   once — as the authoritative PDF.
3. **Internet Archive backfill.** WENRA moved from `wenra.org` to `wenra.eu` and
   the old document tree is gone, so historical material the current view no
   longer lists (the 2007/2008 lists of Reference Levels, the 2009/2011/2013/2014
   RHWG reports, older harmonisation reports) survives only in the Wayback
   Machine. Both domains are enumerated via the CDX API and replayed with
   `/web/{ts}id_/`. Rows whose archived status is 4xx/5xx are dropped while
   revisit rows (`statuscode "-"`) are **kept** — a `statuscode:200` CDX filter
   hides revisits and would truncate the corpus.

`normalize()` extracts PDF text via the shared `common.pdf_extract` helper
(opendataloader-pdf with pdfplumber / pypdf / OCR fallback) and news text from
the node body. Dates come from the view row or news post date, falling back to
the date encoded in the PDF filename and then to the earliest archive capture.
Documents under 400 characters of extracted text (PDF) or 300 (news node) are
dropped, as are CVs, presentations and meeting-logistics PDFs from the legacy
site.

## Usage

```bash
python bootstrap.py test-api             # Connectivity / enumeration test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[© WENRA — All rights reserved](https://wenra.eu/legal-information) — no open
licence is granted.

The wenra.eu legal-information page states *"Copyright © WENRA / All rights
reserved"*. The publications are freely readable and downloadable without
registration and `robots.txt` does not disallow the document tree, but
redistribution — in particular commercial redistribution — is not licensed.
Treat as reference-only pending written permission from WENRA (site published by
Gesellschaft für Anlagen- und Reaktorsicherheit (GRS) gGmbH, Cologne).
