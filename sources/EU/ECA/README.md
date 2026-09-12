# EU/ECA — European Court of Auditors: Reports, Opinions & Reviews

Full-text publications of the **European Court of Auditors** (ECA), the EU's
independent external auditor:

- **Special reports** (`SR-YYYY-NN`) — performance and compliance audits of a
  specific EU policy, programme or body.
- **Annual reports** / **Annual reports on EU agencies** (`AR`, `SAR`,
  `AGENCIES`) — the annual statement of assurance on the EU budget and on the
  agencies' accounts.
- **Opinions** (`OP`) — the ECA's formal Treaty opinions on draft EU legislation
  with a financial impact.
- **Reviews / Landscape reviews / Rapid case reviews** (`RW`, `INSR`, `RCR`) —
  analytical overviews of a policy area.

These are official EU documents = **doctrine**.

## How it works

1. **Enumerate** every published PDF through the ECA site's public **SharePoint
   Search REST** endpoint (`/_api/search/query`), across the modern
   `/ECAPublications/*` library and the historical `/Lists/ECADocuments/*`
   archive. Each publication is issued in ~24 language variants; only the
   English one (path suffix `_EN.pdf`) is kept.
2. **Extract** full text from each born-digital PDF with **PyMuPDF** (a
   minimum-length guard skips the rare pre-2000 scanned document).
3. **Normalize** to the standard schema. `_id` is the ECA document code (e.g.
   `EU/ECA/SR-2026-14`); `year` is derived from the code, `url` is the direct
   PDF link on `www.eca.europa.eu`.

Reachable over plain HTTPS (HTTP 200) — no WAF, JS challenge or authentication.

## Additive, not a duplicate

`EU/OJC-Acts` (CELLAR sector-3 `Y`) carries only the short OJ **C-series
summaries** of a handful of ECA special reports. This source stores the *full*
report / opinion / review bodies, and uses the ECA document code as `_id`, so
there is no CELEX `_id` collision.

## Usage

```bash
python3 sources/EU/ECA/bootstrap.py test              # probe: enumerate + extract one
python3 sources/EU/ECA/bootstrap.py bootstrap --sample # 15 sample records
python3 sources/EU/ECA/bootstrap.py bootstrap          # full run -> data/records.jsonl
```

## License

[EU institutional reuse — Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — ECA publications are documents of an EU institution, reusable including for commercial purposes; attribution to the source is requested.
