# US/NC-OAH — North Carolina Office of Administrative Hearings (ALJ Decisions)

Full text of Administrative Law Judge (ALJ) contested-case **Final
Decisions** from the North Carolina Office of Administrative Hearings
(OAH), the State's central, independent quasi-judicial tribunal. An ALJ
hears a contested case between a person and a State agency and issues a
decision resolving that specific case — i.e. **case_law**.

- **Publisher:** North Carolina Office of Administrative Hearings (OAH)
- **Index:** https://www.oah.nc.gov/administrative-law-judge-decisions
- **Type:** `case_law`
- **Jurisdiction:** US-NC (North Carolina)
- **Auth:** none · no JavaScript · no CAPTCHA

## How it works

1. GET the archive index — a Drupal page that links to 25 per-agency,
   per-year listing pages named `/{agency-slug}-decisions-{YEAR}`
   (chiefly Alcoholic Beverage Control Commission contested cases,
   2000–2016, plus a few professional-licensing boards).
2. GET each listing page and collect the decision-document links
   `/documents/files/alj/{doc-slug}/download` (a literal `blank` slug is
   a placeholder and is skipped).
3. Download each document and extract its text:
   - **PDF** (`%PDF`) → `common.pdf_extract` (born-digital text layer).
   - **Legacy Word 97-2003** (`.doc` / OLE, `D0 CF 11 E0`) → a
     pure-python OLE **piece-table** reader (`olefile`), decoding both
     the ANSI (cp1252) and Unicode (utf-16-le) pieces.

Of the ~162 archive documents, ~20 are PDFs and ~142 are legacy `.doc`
files, so `.doc` support is essential to the corpus.

## Coverage / follow-up

This covers the public **archive** only. Decisions from **2017-present**
are exposed exclusively through the ASP.NET case-management search portal
(`https://www.encoah.oah.state.nc.us/publicsite/search.aspx`), a
ViewState/AJAX form that a plain GET cannot drive. Harvesting the modern
corpus (all agency categories — DHHS/Medicaid, DMV, State Personnel,
etc.) is a documented follow-up requiring a ViewState-driven POST scraper
or browser automation.

## Dependencies

- `curl` (fetching), the shared `common.pdf_extract` (PDF text), and
  `olefile` (legacy `.doc` extraction — small pure-python package).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~160 decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105 analogue (government edicts)](https://www.law.cornell.edu/uscode/text/17/105) — Final Decisions of the North Carolina Office of Administrative Hearings are official North Carolina state-government works in the public domain. Commercial use permitted; no attribution required.
