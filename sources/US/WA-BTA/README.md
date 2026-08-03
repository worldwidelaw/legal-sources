# US/WA-BTA — Washington State Board of Tax Appeals (Decisions)

Full text of the decisions of the **Washington State Board of Tax Appeals
(BTA)** — Washington's independent, quasi-judicial administrative forum
(RCW 82.03) that hears appeals from county Boards of Equalization and the
Washington Department of Revenue. Matters include property-tax valuation
and exemption, excise/use tax, forest-land classification and related state
and local tax disputes (taxpayer v. Department of Revenue / county
assessor). Each Final Decision or Order resolves a specific tax
controversy, so the corpus is **case_law**.

## Source

- Decisions page: https://bta.wa.gov/index.php/decisions-3/
- Document store: `https://apps.bta.wa.gov/Decision PDF/...`
- Search engine: dtSearch Web — `https://apps.bta.wa.gov/dtSearch/dtisapi6.dll`

## How it works

1. **Discover** — a single POST to the dtSearch ISAPI (`cmd=search`,
   `request=board`, the live "Decision PDF" index alias, `maxFiles=25000`)
   returns the direct PDF URL of every indexed decision (~19,476 documents,
   from the oldest numeric dockets through the current Formal Dockets). The
   result rows also expose the indexed Title and file Date.
2. **Fetch** — each decision PDF is downloaded and its text layer extracted
   via `common.pdf_extract` (born-digital, no OCR needed).
3. **Normalize** — the decision date is taken as the latest in-document
   "Month D, YYYY" (the dtSearch file date is only a re-indexing date for
   older documents); the appellant/case caption is parsed from the PDF
   first page. Output is the standard `case_law` schema with full `text`.

No JavaScript, no CAPTCHA, no authentication.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample decisions
python bootstrap.py bootstrap           # full pull (all decisions)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Washington State Board of Tax Appeals are official quasi-judicial government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
