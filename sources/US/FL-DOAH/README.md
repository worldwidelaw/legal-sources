# US/FL-DOAH — Florida Division of Administrative Hearings (Recommended & Final Orders)

Full text of the orders issued by Administrative Law Judges of the
**Florida Division of Administrative Hearings (DOAH)** — Florida's
central, independent administrative tribunal (the "central panel").

Under Chapter 120, Florida Statutes (the Administrative Procedure Act),
a DOAH ALJ hears a contested case between a person and a State agency
and issues a **Recommended Order** (later adopted by the agency as a
Final Order), a **Summary Final Order**, or a **Final Order** directly.
Each order resolves a specific contested case → `case_law`. DOAH hears
matters for essentially every Florida executive agency (professional
licensing, environmental permitting, Medicaid, child support, teacher
certification, bid protests, education, and more).

## Access

No JavaScript, no CAPTCHA, no authentication.

The order archive is served as **browsable IIS directory listings**,
one directory per year, at:

```
https://www.doah.state.fl.us/ROS/{YEAR}/       # 1975 – present
```

Each listing links the order PDFs at:

```
https://www.doah.state.fl.us/ROS/{YEAR}/{casedigits}[suffix].pdf
```

where `{casedigits}` is the 8-digit DOAH case number `YYNNNNNN`
(e.g. `25000021.pdf` == case `25-000021`). A duplicate
electronically-signed copy sometimes exists as
`{casedigits}_282_<date>_<n>_e.pdf`; when a plain copy of the same case
exists, the `_e` duplicate is skipped. `Amended` revisions are kept as
distinct records.

The orders are **born-digital text-layer PDFs** (verified back to
1975); text is extracted with `common.pdf_extract`. The DOAH case
number (with its type-suffix letters, e.g. `26-1139PL`), the order
type, the decision date, and the parties are parsed from the order
body.

## Coverage

- **~41,570 order PDFs** across 1975–present (e.g. 2025 ≈ 584 orders,
  2005 ≈ 1,053).
- All Florida executive-agency contested cases handled by DOAH.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 recent samples
python bootstrap.py bootstrap            # full pull (1975–present)
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Orders of the Florida Division of Administrative Hearings are official Florida state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
