# US/IN-EthicsOpinions — Indiana State Ethics Commission Advisory Opinions

Full text of the **formal advisory opinions** of the [Indiana State Ethics
Commission](https://www.in.gov/ig/), administered by the Indiana Office of
Inspector General, construing the Code of Ethics for state officers, employees
and special state appointees (Ind. Code 4-2-6 and 42 IAC 1).

On request, the Commission issues a written opinion at a monthly public meeting
applying the ethics code to specific circumstances. Each opinion is public and
is the Commission's authoritative interpretation — **doctrine**. About **408**
opinions (1988–present).

## Access

No auth, no CAPTCHA, no JavaScript. The opinions index links one listing page
per year:

- **Index:** `https://www.in.gov/ig/opinions/` → year slugs `advisory-opinions-{YYYY}`.
- **Year page:** `https://www.in.gov/ig/opinions/advisory-opinions-{YYYY}/` (301-redirects to trailing slash) → born-digital PDF links.
- **Document:** `https://www.in.gov/ig/files/opinions/{YYYY}/{filename}.pdf`.

Two filename schemes: pre-2020 `s{YY}-I-{N}_{tags}.pdf` (number `YY-I-N`) and
2020+ `{YYYY}-FAO-{NNN}-{agency}-REDACTED.pdf` (number `YYYY-FAO-NNN`). Full text
is extracted via the shared `common.pdf_extract` chain; the issue date is parsed
from the `Month DD, YYYY` line in the PDF body.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity + extraction test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — no restrictions.

Formal advisory opinions of the Indiana State Ethics Commission are official
public records of an Indiana state body interpreting statute and administrative
rule (government-edict works), issued at public meetings and published for public
use under the Code of Ethics (Ind. Code 4-2-6, 42 IAC 1). Commercial use
permitted; no attribution required.
