# US/OH-SERB — Ohio State Employment Relations Board (Opinions)

Full-text opinions of the **Ohio State Employment Relations Board (SERB)**, the
state agency that administers Ohio's Public Employees' Collective Bargaining Act
(**Ohio Revised Code Chapter 4117**). SERB adjudicates unfair-labor-practice
charges, representation / certification petitions, and bargaining-unit
determinations for Ohio public employers and employee organizations. Each
numbered SERB Opinion resolves a specific contested case → `case_law`.

## Coverage

~480 opinions, **1984 through 2018** (SERB Opinion numbering).

## Build recipe

`serb.ohio.gov` migrated to a modern InnovateOhio/Akamai platform that blocks
datacenter enumeration, and the born-digital opinion tree is no longer directly
browsable. However, the full SERB Opinions corpus was preserved by the
**Internet Archive Wayback Machine** under:

```
http://www.serb.ohio.gov/pdf/opinions/{YEAR}/{name}.pdf
```

The scraper:

1. Enumerates all preserved opinion PDFs via the Wayback **CDX API**
   (`url=serb.ohio.gov/pdf/opinions*`, `filter=statuscode:200`,
   `collapse=urlkey`) → ~482 unique PDFs.
2. Downloads each preserved PDF via the `/web/{timestamp}id_/{url}` raw-replay
   endpoint.
3. Extracts full text with `common.pdf_extract` — most opinions are
   born-digital; a minority are scanned and fall back to OCR (run with
   `tesseract` on `PATH`).
4. Parses the SERB opinion number and internal case number from the body, and
   the year from the file path.

No auth, no CAPTCHA. Run with a Python that has `fitz`/PyMuPDF (and, for the
scanned minority, `pytesseract` + `tesseract`) available — e.g. `/usr/bin/python3`
with `PATH=/opt/homebrew/bin:$PATH`.

```bash
python bootstrap.py test-api           # connectivity + one-record extraction
python bootstrap.py bootstrap --sample # ~12 sample records
python bootstrap.py bootstrap          # full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — opinions of the Ohio State Employment Relations Board are official works of Ohio state government (edicts of a government agency) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially.

Retrieved via the Internet Archive Wayback Machine's preservation of the
official `serb.ohio.gov` opinion corpus.
