# US/ME-MLRB — Maine Labor Relations Board (Decisions)

Full text of the published decisions of the **Maine Labor Relations Board
(MLRB)**, Maine's independent quasi-judicial labor-relations agency. The MLRB
adjudicates public- and private-sector labor disputes under Maine's several
labor-relations statutes: prohibited practice complaints (PPC), unit
determination / representation and clarification cases (UD / UC / UDA / IR),
interpretive rulings, and related matters. The corpus also includes the
Superior Court and Law Court appellate decisions that review MLRB orders.

Each decision resolves a specific contested case, so every record is
`case_law`.

## Source

- **Index:** https://www.maine.gov/mlrb/decisions/summaries — a single
  server-rendered "Decision Summaries" page listing every case *from May 2006
  through the present* (and the court appeals of those cases).
- Each entry is an `<h2>` heading — `{date}, {party caption}, {Case|Docket}
  No. {NN-XXX-NN} or {NN-XXX-NN} (pdf)` — whose **(pdf)** link points at the
  full decision PDF under `/mlrb/sites/maine.gov.mlrb/files/inline-files/`,
  followed by a `<p>` plain-language summary.

## Method

1. `GET` the summaries page.
2. For each `<h2>` carrying a `.pdf` link, parse the leading decision date,
   the party caption, and the case number (from the PDF filename).
3. Download the PDF and extract its text via the shared `common.pdf_extract`
   (opendataloader → pdfplumber → OCR fallback).
4. Normalize into the `case_law` schema.

No JavaScript, no CAPTCHA, no authentication. Cases earlier than May 2006 are
reachable only through the site's Google-CSE search box (browser-bound) and are
out of scope for this deterministic index scrape.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain — U.S. Government Work (17 U.S.C. § 105 analogue)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Maine Labor Relations Board and the reviewing Maine courts are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
