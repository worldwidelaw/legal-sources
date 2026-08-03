# UK/ScottishLandCourt — The Scottish Land Court

Significant decisions (2007–present) of **The Scottish Land Court** (*An Cùirt
Fearainn*), the Scottish court of record that determines disputes about
agricultural tenancies, crofting, common grazings and related land matters under
the Crofters (Scotland) Act 1993, the Agricultural Holdings (Scotland) Acts, the
Land Reform (Scotland) Acts and related legislation.

- **Type:** case_law
- **Jurisdiction:** Scotland (GB-SCT) — not covered by `UK/CaseLaw` (England &
  Wales + reserved UK tribunals). The Scottish Land Court is a **distinct** body
  from `UK/LandsTribunalScotland` (valuation / title conditions).
- **Source:** http://www.scottish-land-court.org.uk/decisions/recent-decisions
- **Auth:** none
- **Corpus:** ~303 full-text significant decisions, born-digital HTML.

## How it works

The Court publishes its "Significant decisions 2007 to date" as a single
server-rendered HTML table. Each row carries the party names, the neutral
citation (e.g. `[2026] SLC 7`) and the date issued, and links to a standalone
HTML decision document at `/decisions/{slug}` containing the full opinion text.

Two clean, born-digital layouts occur, both handled by extracting the `<body>`
text:

1. **Modern decisions** — a `<header>` (court, parties, case reference, panel,
   date) followed by a `<main>` with the numbered opinion paragraphs.
2. **Older "reported" decisions** (`slug` ending `.rub`) — a single rubric
   report (headnote + full opinion) directly in `<body>`.

No OCR is required.

### Access note

The site's HTTPS certificate does not match the hostname, so the scraper fetches
over plain **HTTP** (`http://www.scottish-land-court.org.uk`). The content is
fully public; no authentication is involved.

## Usage

```bash
python bootstrap.py bootstrap --sample   # sample records for validation
python bootstrap.py bootstrap            # full pull
python bootstrap.py update               # incremental (recent decisions)
python bootstrap.py test                 # connectivity check
```

## License

> ⚠️ **Commercial use restricted.** No explicit open licence is published on the
> Court's website; the decisions are Crown-copyright public judicial records.
> Commercial re-use is conservatively flagged, consistent with the sibling
> GB-SCT sources (`UK/ScotHousingChamber`, `UK/ScotTaxChamber`).

[Scottish Land Court website](http://www.scottish-land-court.org.uk/) — public
judicial records (Crown copyright); personal / in-house use only.
