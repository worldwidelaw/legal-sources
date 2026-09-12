# US/EPA-EAB — U.S. EPA Environmental Appeals Board Decisions & Orders

Full text of the final agency adjudicative **decisions and orders** of the
U.S. EPA **Environmental Appeals Board (EAB)** — the appellate tribunal within
the Environmental Protection Agency that decides administrative appeals of:

- **permit decisions** — PSD / Title V (Clean Air Act), NPDES (Clean Water
  Act), RCRA hazardous-waste, UIC underground-injection, and ocean-dumping
  permits; and
- **civil administrative penalty / compliance orders** — under the CAA, CWA,
  RCRA, SDWA, FIFRA, TSCA, EPCRA, and related statutes.

Each order resolves a specific docketed appeal (e.g. `CAA 26-05C`,
`NPDES 25-01`, `FIFRA 26-02C`) → `case_law`.

## Access

No JavaScript, CAPTCHA, or authentication. The Board publishes its docket as a
public **Lotus Domino** database:

```
https://yosemite.epa.gov/oa/EAB_Web_Docket.nsf
```

- **Enumeration** — Domino's structured XML feed `?ReadViewEntries` over the
  decision views: `Closed+Dockets`, `Unpublished~Final~Orders`,
  `Significant Interlocutory Decisions`, and
  `EAB Decisions Reviewed by the Federal Courts`. Each entry is identified by a
  32-hex Domino **UNID**.
- **Document** — `GET /oa/EAB_Web_Docket.nsf/0/{UNID}?OpenDocument`. Closed
  docket pages carry an *Index of Filings*; the Board's order/decision filings
  attach the PDF, while the final-order views attach the PDF directly.
- **Order PDF** — `/oa/EAB_Web_Docket.nsf/.../{filingUNID}/$File/{name}.pdf`,
  born-digital (real text layer → PyMuPDF, no OCR).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample decisions
python bootstrap.py bootstrap            # Full pull -> data/records.jsonl
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Output

Normalized `case_law` records include `_id` (UNID-based), `title` (case
caption), `text` (full decision text), `date`, `appeal_number`,
`docket_number`, `statute` (program code), `court`, and `url`. Sample records
range ~3K–115K characters of clean full text.

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— works of the U.S. federal government are not subject to copyright.
Commercial use permitted.
