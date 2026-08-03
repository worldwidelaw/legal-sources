# US/SC-ALC — South Carolina Administrative Law Court (ALC) Decisions

Orders and final decisions of the **South Carolina Administrative Law Court
(ALC)** — formerly the Administrative Law Judge Division — the state's
centralized court of record that hears contested cases and appeals from most
South Carolina state agencies:

- Department of Revenue (state **tax appeals**)
- Department of Health and Environmental Control (DHEC)
- Department of Insurance
- Department of Motor Vehicles (DMV)
- Department of Labor, Licensing and Regulation (LLR)
- Department of Social Services (DSS)
- alcoholic-beverage licensing, and many others

Each order resolves a specific contested case → `case_law`.

## Data access

No auth, no CAPTCHA. Decisions are published through a Kendo/Telerik search
portal at `https://www.decisions.scalc.net/`. The search grid is
JavaScript-driven, but every individual decision document is served **directly
by a plain sequential integer id**, with no cookie or session:

```
https://www.decisions.scalc.net/Home/ViewPdf/{id}
```

The response is one of two shapes, both carrying the full decision body:

- **older decisions** → an HTML fragment (UTF-16LE encoded, mislabelled
  `Content-Type: application/pdf`) whose body is the decision text.
- **newer decisions** → a genuine born-digital `%PDF` document.

`bootstrap.py` walks `id = 1 .. N` (N > 14,000 in 2026), skips the `HTTP 500`
gaps, decodes the HTML fragments (strip tags) or extracts the PDFs via the
shared `common.pdf_extract` extractor, and parses the order type, docket number
(`YY-ALJ-NN-NNNN-XX`) and decision date out of the body text.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull (id 1..ceiling)
```

Use an interpreter with `PyMuPDF`/`pdfplumber` installed (e.g. `/usr/bin/python3`
on the build host) so the newer PDF decisions extract cleanly.

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the South Carolina Administrative Law Court are official works of South Carolina state government (edicts of a state court of record) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
