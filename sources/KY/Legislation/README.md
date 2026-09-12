# KY/Legislation — Cayman Islands Legislation

**Source:** [https://legislation.gov.ky/cms/](https://legislation.gov.ky/cms/)
**Data types:** legislation

Official consolidated laws of the Cayman Islands, maintained by the Office of the
Law Revision Commissioner: principal Acts, subordinate legislation (regulations,
orders, rules) and amending instruments, 1961–present. Every revision of an item
is captured, so the Companies Act contributes ~20 records from its 1995 Revision
through its 2026 Revision.

## Access

Enumeration goes through the CMS index pages, not the file tree:

| Page | How |
|------|-----|
| `/cms/legislation/current/by-title.html` | POST once per letter, `submit4=A`…`Z` |
| `/cms/legislation/repealed.html` | GET |
| `/cms/legislation/revoked-secondary-legislation.html` | GET |
| `/cms/legislation/not-in-force-menu.html` | GET |

Each row links the in-force PDF and opens a "legislation history" modal listing
every earlier revision; both are harvested. Full text comes from downloading each
PDF and extracting it with pdfplumber/pypdf. About 1,764 distinct PDFs.

The Apache directory listings under `/cms/images/LEGISLATION/` that an earlier
version of this scraper crawled now return an empty index (`<ul></ul>`), which is
why enumeration yielded 0 records — see issue #1365.

Notes:

- `_g.pdf` files are the gazette-typeset reprint of the same consolidated text and
  are skipped as duplicates.
- Crawl order is newest-revision-first across items, principal Acts before
  subordinate and amending, so a run cut short still covers the corpus broadly.
- A handful of pre-1995 amending instruments are scanned images and need OCR.

## License

Official Cayman Islands legislation — published by the Cayman Islands Government. Crown Copyright (Cayman Islands) applies. Free public access to consolidated laws.
