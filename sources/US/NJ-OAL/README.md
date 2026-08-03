# US/NJ-OAL — New Jersey Office of Administrative Law (ALJ Decisions)

Full text of Administrative Law Judge **Initial** and **Final Decisions** of
the New Jersey Office of Administrative Law (OAL), New Jersey's central,
independent tribunal for contested cases between persons and State agencies
(Education / special-education due process, Medicaid / Medical Assistance,
Civil Service, Children & Families, Banking & Insurance, Public Utilities,
Labor, Environmental Protection, and more). Each decision resolves a specific
contested case = **case_law**.

## Source

- Archive (Rutgers Law): https://njlaw.rutgers.edu/OAL/
- Search backend: `https://njlaw.rutgers.edu/OAL/search.php?q={term}&limit=5000`
- Documents: `https://njlaw.rutgers.edu/OAL/NJLAW-pdfs/{initial|final}/{docket}.pdf`
- Coverage: October 1997 – present, 10,000+ decisions.

## How it works

1. The search backend honours an undocumented `limit` param that removes the
   default result cap. Querying every agency transmittal code (`edu`, `hma`,
   `csv`, `eds`, `caf`, …) plus a handful of broad legal terms and unioning the
   `NJLAW-pdfs/*/*.pdf` hrefs enumerates the whole corpus.
2. Each decision is a born-digital, text-layer PDF; text is extracted with
   `common.pdf_extract` (pdfplumber). A `<200`-char guard skips empty/placeholder
   documents.
3. The docket prefix (first 2–4 letters) maps to the transmitting State agency;
   the trailing `-YY` yields the docket year (fallback date). The body's
   `Decided: <Month D, YYYY>` is preferred for the decision date.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction smoke test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain — 17 U.S.C. § 105 (US government work)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the New Jersey Office of Administrative Law are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
