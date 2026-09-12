# VG/Gazette — The Virgin Islands Official Gazette

Full text of the Official Gazette of the Government of the British Virgin
Islands, published weekly on Thursdays (plus Extraordinary editions) by the
Gazette Unit of the Cabinet Office.

- **Publisher:** Gazette Unit, Cabinet Office, Government of the Virgin Islands
- **Portal:** https://eservices.gov.vg/gazette/
- **Type:** `legislation`
- **Language:** English
- **Coverage:** 9 November 2006 (edition G00001, the start of the online
  archive) to the present

## What is collected

Part 1 of each edition — **Government and Statutory Notices** — which carries
statutory instruments, proclamations, commencement notices, subsidiary
legislation, appointments to boards and commissions, and land, court, election
and deportation notices. The **attachments** circulated with the Gazette (the
Acts, Bills and Statutory Instruments themselves) are collected as separate
documents.

## How it is accessed

The portal's browse and search pages are **login-gated**: `/gazette/content/recent-gazettes`
returns HTTP 403 to an anonymous client, gazette taxonomy pages report "no
content classified with this term", and the gazette nodes 403. Registration is
free, but this project only ingests genuinely open data, so no account is used.

The free tiers are nonetheless served as static files from the site's public
Drupal file directory and need no session:

| Directory | Contents |
|---|---|
| `/sites/eservices.gov.vg.gazette/files/governmentandstatutorynotices/` | Part 1 editions |
| `/sites/eservices.gov.vg.gazette/files/archiveattachments/` | Acts, SIs, Orders |

Part 1 of the archive is **enumerable**: editions are numbered `G00001.pdf`
upwards, one file per edition, verified live from `G00001` (9 November 2006)
through `G00767` (late 2015). Isolated numbers 404 — those are genuine gaps in
the archive, and the scraper tolerates them while failing loud on a long
unbroken run of failures, which would mean the vantage is being refused rather
than the archive being sparse.

Post-2015 editions moved to a free-form `#<issue> <date> Part 1.pdf` name that
cannot be derived (the issue number drifts against the publication date because
Extraordinary editions consume numbers). Those, and the attachment file names,
are carried as seed lists harvested from the Internet Archive's index of the
same public directory. **Extend `SEED_NOTICES` / `SEED_ATTACHMENTS` in
`bootstrap.py` whenever new file names surface.**

Text is extracted from the born-digital PDFs via `common/pdf_extract`. Some
older editions were typeset with overlapping glyphs, so the masthead reads as
e.g. `THHURSDAY 110 JANUARRY 2013`; the date parser collapses doubled
characters (no English month name contains a doubled letter) before matching,
and falls back to the date in the file name.

## Not collected — and why

`/sites/eservices.gov.vg.gazette/files/LiquidationandOtherNotices/` holds
Part 2 of each edition. The Gazette's own Help page states that after two
months only Government and Statutory Notices and attachments remain free of
charge, and that the Liquidation and Other Notices archive requires a **$500
per year enhanced subscription**. Those files do answer without a session, but
fetching them would circumvent a paywall, so the scraper never requests that
directory.

## Usage

```bash
python bootstrap.py test               # Connectivity check
python bootstrap.py bootstrap --sample # 10 sample records
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap-fast     # Concurrent full pull (VPS pipeline)
```

## Record schema

| Field | Description |
|---|---|
| `_id` | `VG/Gazette/{doc_id}` |
| `doc_id` | `G00591`, or the attachment/edition file stem |
| `title` | Edition title with publication date, or the instrument's title |
| `text` | Full extracted text of the PDF |
| `date` | Publication date, ISO 8601 |
| `url` | Direct link to the official PDF |
| `gazette_part` | Always `Government and Statutory Notices` |
| `document_kind` | `notices` or `attachment` |

## License

> ⚠️ **No formal open licence is published.** Crown Copyright (BVI). Only the
> tiers the Gazette Unit publishes free of charge are collected; the paid
> Part 2 archive is excluded (see above).

[Crown Copyright (British Virgin Islands)](https://eservices.gov.vg/gazette/content/faqs)
— the Gazette Unit's FAQ and Help pages state that Government and Statutory
Notices and Gazette attachments are "available free of charge" online.
Statutory instruments and Acts published in the Gazette are official law and
are freely reproducible as such. Attribution to the Virgin Islands Official
Gazette is expected.
