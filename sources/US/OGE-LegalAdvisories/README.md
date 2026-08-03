# US/OGE-LegalAdvisories — U.S. Office of Government Ethics Legal Advisories

Full text of the **Legal Advisories** (and legacy **DAEOgrams**) issued by the
**U.S. Office of Government Ethics (OGE)**, the supervising ethics office for the
federal executive branch (Ethics in Government Act; 5 C.F.R. part 2600).

Each advisory is OGE's written, authoritative interpretation of the federal
criminal conflict-of-interest statutes (**18 U.S.C. §§ 202–209**), the Standards
of Ethical Conduct (**5 C.F.R. part 2635**), the executive-branch financial-
disclosure rules, and post-employment restrictions — addressed to Designated
Agency Ethics Officials government-wide and relied on by agencies. This is
official federal legal interpretation → **doctrine**.

## Source

- **Publisher:** U.S. Office of Government Ethics
- **View:** https://www.oge.gov/web/oge.nsf/Legal%20Advisories
- **Full view (all docs):** `https://www.oge.gov/web/oge.nsf/Legal%20Advisories?OpenView&Count=2000`
- **Coverage:** ~470 advisories, `LA-YY-NN` (current) and `DO-YY-NNN` / `YYxNN`
  legacy DAEOgrams, 1997–present

## Access method

1. GET the Lotus Domino (.nsf) web view with the `?OpenView&Count=2000` query —
   the bare view URL renders only the ~27 most recent rows, so the `Count`
   parameter is required to enumerate the full corpus. Each entry is an
   `<a href=".../$FILE/{file}.pdf?open">` wrapping the `<div id="vt">` title,
   followed by a `<div id="vtxt">` summary and a `<div id="vdt">` date
   (`MM/DD/YYYY`).
2. Download each advisory's born-digital PDF and extract its text layer via the
   shared `common.pdf_extract` backend — no OCR is required.
3. Take the issue date from the view's date column, with a fallback to the
   two-digit year embedded in the document number.

No JavaScript, CAPTCHA, or authentication. Builds locally.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all legal advisories)
```

## License

[Public Domain (US Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) —
Legal Advisories of the U.S. Office of Government Ethics are works of the U.S.
federal government in the public domain. No attribution required; commercial use
permitted.
