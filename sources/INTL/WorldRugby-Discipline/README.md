# INTL/WorldRugby-Discipline — World Rugby Disciplinary & Judicial Decisions

Full-text published decisions of World Rugby's independent disciplinary process:
Judicial Committee / Judicial Officer decisions, Disciplinary Committee
decisions, Foul Play Review Committee (FPRC) decisions, anti-doping decisions,
appeal decisions, and the CAS awards rendered on appeal. Includes Rugby World
Cup and international-window matters.

## Data source

- **Hub (single server-rendered page):**
  `https://www.world.rugby/organisation/governance/discipline/decisions`
- The page links every published decision PDF directly. PDFs are hosted on
  `pulse-static-files.s3.amazonaws.com` and `resources.world.rugby` under a
  `/document/YYYY/MM/DD/<uuid>/<descriptive-filename>.pdf` path.
- The decision **date** is taken from the `/document/YYYY/MM/DD/` path segment;
  the descriptive **filename** supplies the title (parties + decision type).
- Each PDF is downloaded and its full text extracted via the shared
  `common.pdf_extract` helper (opendataloader / pdfplumber / pypdf fallback).

~178 decisions span 2019–2025 (no 2020 — the COVID-affected season). No login,
no WAF; reachable from a normal IP.

## Type

`case_law` — disciplinary and judicial tribunal decisions.

## Usage

```bash
python bootstrap.py test               # list discovered decision PDFs
python bootstrap.py bootstrap --sample # fetch a sample with full text
python bootstrap.py bootstrap          # full pull
```

## License

> ⚠️ **Commercial use restricted.** Published openly for transparency, but
> World Rugby asserts copyright with no open licence.

[World Rugby Terms & Conditions](https://www.world.rugby/organisation/about-us/terms-conditions) — All rights reserved; attribution expected.
