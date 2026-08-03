# US/USDA-AgricultureDecisions — USDA Agriculture Decisions

Full text of decisions and orders issued in U.S. Department of Agriculture
adjudicatory proceedings, published in the official **Agriculture Decisions**
reporter. Covers both:

- **OALJ initial decisions** — the USDA Office of Administrative Law Judges, and
- **Judicial Officer (JO) final decisions** — the Secretary of Agriculture's
  Judicial Officer, who issues the Department's final agency decisions on appeal.

Cases arise under the Packers & Stockyards Act, Animal Welfare Act, Perishable
Agricultural Commodities Act (PACA), Agricultural Marketing Agreement Act, Horse
Protection Act, Organic Foods Production Act, the meat/poultry inspection acts,
plant/animal quarantine statutes, and other USDA-administered laws. Each entry
resolves a specific contested case → `case_law`.

## Access

The official USDA OALJ site (`oalj.oha.usda.gov` → `www.usda.gov/oha/oalj`) is
unreachable from most build vantages (HTTP/2 `INTERNAL_ERROR` / connect-timeout).
The full published run is mirrored — born-digital, with a text layer — by the
**National Agricultural Law Center** (a USDA-funded academic center) as
semi-annual **compilation PDFs**:

```
https://nationalaglawcenter.org/wp-content/uploads/assets/agdecisions/VOLUME-{NN}-BOOK-{N}.pdf
```

where `NN` is the volume number (≈ Vol 55 = 1996 … Vol 78 = 2019) and `N` is the
book (Book One = Jan–Jun, Book Two = Jul–Dec). Available on the mirror: Vol 55
and Vols 59–78 (both books each); a few volumes 404 and are skipped.

Each compilation concatenates many individual decisions. Every departmental
decision begins with a caption block:

```
In re: <RESPONDENT NAME>.
Docket No. <NN-NNNN>.
<Decision type>.
Filed <Month DD, YYYY>.
```

and court decisions with `<NAME> v. USDA. No. <docket>. ... Filed <date>.`. The
scraper whitespace-normalizes each volume and splits on that caption (anchored by
the `Filed <date>` line) into one record per decision.

No JavaScript, no CAPTCHA, no auth. Text extracted via PyMuPDF (born-digital
layer, no OCR needed for Vol 59+).

## Usage

```bash
python bootstrap.py test-api            # connectivity + split check
python bootstrap.py bootstrap --sample  # ~12 sample decisions
python bootstrap.py bootstrap           # full pull (all volumes)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Record shape

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `slug`, `case_number`
(docket), `issuer`, `statute`, `volume`, `book`, `title` (case caption), `text`
(full decision body), `url` (source compilation PDF), `date` (filed date, ISO),
`jurisdiction` (`US`).

## License

[US Public Domain (17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105)
— U.S. federal government works (USDA OALJ / Judicial Officer decisions) are in
the public domain under 17 U.S.C. § 105 and the government-edicts doctrine.
Commercial use permitted. Retrieved via the National Agricultural Law Center
mirror, which redistributes the official reporter.
