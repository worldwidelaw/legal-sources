# GM/Judiciary — Judiciary of The Gambia: Law Reports & Judgments

Reported decisions of the superior courts of The Gambia — the Supreme Court,
the Court of Appeal, and the High Court — together with the Cadi (Sharia)
Appeals Panel, published by the **National Council for Law Reporting** on the
official judiciary website, [judiciary.gov.gm](https://judiciary.gov.gm).

## What this source provides

The site publishes bound, multi-case PDF volumes:

- **The Gambia Law Report (2002–2008) Vol. 2** — ~30 reported superior-court
  cases with full judgments.
- **The Sharia Law Report** — ~19 Cadi Appeals Panel decisions (2005–2011).
- **The Gambia Law Reports [1997–2001]** — an index/digest volume (subject
  matter + words & phrases), skipped because it contains no case bodies.

Each PDF volume is a single document containing many cases, so the scraper
**splits every volume into individual reported cases**, each with its own full
text, case name, deciding court, decision date, and (for Sharia) appeal number.

## Access method

1. Scrape `/law-report` and `/sharia-law-report` for `.pdf` links under
   `/sites/default/files/**`.
2. Download each PDF and extract per-page text (PyMuPDF, pdfplumber fallback).
3. Split into cases:
   - **Law Reports:** each case begins with a standalone court-header line
     (`COURT OF APPEAL OF THE GAMBIA`, `SUPREME COURT OF THE GAMBIA`,
     `HIGH COURT OF THE GAMBIA`), preceded by the ALL-CAPS case name.
   - **Sharia Report:** each case begins with `IN THE HIGH COURT OF THE
     GAMBIA … APPEAL NO. AP/N/YYYY … BETWEEN: …`.

### TLS caveat

`judiciary.gov.gm` serves an **incomplete TLS certificate chain** (`unable to
verify the first certificate`). The scraper sets `session.verify = False` for
this host; the PDF endpoints themselves resolve correctly.

## Usage

```bash
python bootstrap.py test              # discover PDFs + extract a few cases
python bootstrap.py bootstrap --sample  # 15 sample cases
python bootstrap.py bootstrap --full    # all cases
```

## License

[Government Edict — Public Domain](https://judiciary.gov.gm/) — court
judgments and official law reports are edicts of the Government of The Gambia
and are not subject to copyright. Published on the official judiciary domain by
the National Council for Law Reporting. Commercial use permitted; no
attribution required.
