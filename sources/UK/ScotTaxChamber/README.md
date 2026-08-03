# UK/ScotTaxChamber — First-tier Tribunal for Scotland Tax Chamber

Written decisions of the **First-tier Tribunal for Scotland Tax Chamber**
(FTS Tax Chamber), the devolved Scottish tribunal that decides appeals against
decisions of **Revenue Scotland** on the fully devolved Scottish taxes:

- **Land and Buildings Transaction Tax (LBTT)** — the Scottish successor to Stamp
  Duty Land Tax;
- **Scottish Landfill Tax (SLfT)**;

and related penalty, review and information-notice matters under the **Revenue
Scotland and Tax Powers Act 2014**. Its administration is provided by the
**Scottish Courts and Tribunals Service (SCTS)**.

These are adjudicative **case law** for the **Scotland (GB-SCT)** jurisdiction.
They are **not** covered by `UK/CaseLaw` (which indexes England & Wales superior
courts and reserved UK tribunals via the National Archives Find Case Law service;
FTS Tax Chamber decisions are not on that service).

## Source

- **Site:** https://taxtribunals.scot/
- **Coverage:** ~70+ decisions, 2016–present (neutral citation `[YYYY] FTSTC N`)
- **Auth:** none (free public access)

## How it works

The homepage is a single server-rendered page that embeds one
`<table class="decision-table-summary">` per decision. Each table carries the
metadata rows — Appellant, Respondent, Tribunal members, Decision Date,
Application Type, Tax Type, and Notes (subject) — and a *Decision Document* row
linking to the born-digital decision PDF at `decisions/[YYYY] FTSTC N.pdf`.

The scraper parses each table, downloads each PDF, and extracts the full text
with PyMuPDF (pdfplumber/pypdf fallback). No OCR is required — the PDFs are
born-digital. One record per decision.

```
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent decisions)
python bootstrap.py test               # Connectivity/extraction test
```

## License

> ⚠️ **Commercial use restricted.** See terms below.

[SCTS website terms of use](https://www.scotcourts.gov.uk/terms-of-use) — the
Scottish Courts and Tribunals Service permits reproduction of judgments and
decisions for personal and in-house use, but restricts commercial re-use without
consent. Same basis as `UK/ScotHousingChamber`. Not published under the Open
Government Licence.
