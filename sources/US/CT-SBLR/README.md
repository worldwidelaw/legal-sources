# US/CT-SBLR — Connecticut State Board of Labor Relations (Decisions)

Full-text decisions of the **Connecticut State Board of Labor Relations
(SBLR / CSBLR)**, the tribunal within the Connecticut Department of Labor that
adjudicates public- and private-sector labor-relations disputes under:

- the **State Employees Relations Act (SERA)** — state employees,
- the **Municipal Employee Relations Act (MERA)** — municipal employees, and
- the **Connecticut Labor Relations Act (CLRA)** — private sector.

The Board decides prohibited-practice complaints, representation / election
petitions, and bargaining-unit determinations. Each numbered decision resolves a
specific contested case → `case_law`.

## Coverage

~5,672 decisions, **Decision No. 1 (1945) through the 5300s (2025)** — the full
born-digital corpus.

## Build recipe

The live archive at `dolpublicdocumentlibrary.ct.gov` sits behind an F5/Shape
(TSPD) JavaScript bot-challenge and cannot be enumerated without a browser, and
the retired `ctdol.state.ct.us` site now 301-redirects every `/csblr/` path to a
`portal.ct.gov` 404. However, the entire decision corpus was fully preserved by
the **Internet Archive Wayback Machine** under the stable path:

```
http://www.ctdol.state.ct.us/csblr/decisions-pdf/{YEAR}/{name}.pdf
```

The scraper:

1. Enumerates all preserved PDFs via the Wayback **CDX API**
   (`url=ctdol.state.ct.us/csblr/decisions-pdf*`, `filter=statuscode:200`,
   `collapse=urlkey`) → ~5,672 unique decision PDFs.
2. Downloads each preserved PDF via the `/web/{timestamp}id_/{url}` raw-replay
   endpoint (serves the original bytes, no JS challenge).
3. Extracts full text with `common.pdf_extract` (born-digital PDFs, no OCR).
4. Parses decision number + year from the file path, and case number + decision
   date from the decision body.

No auth, no CAPTCHA, no JS challenge. Run with a Python that has `fitz`/PyMuPDF
available (e.g. `/usr/bin/python3` on the build host).

```bash
python bootstrap.py test-api           # connectivity + one-record extraction
python bootstrap.py bootstrap --sample # ~12 sample records
python bootstrap.py bootstrap          # full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Connecticut State Board of Labor Relations are official works of Connecticut state government (edicts of a government agency) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially.

Retrieved via the Internet Archive Wayback Machine's preservation of the
official `ctdol.state.ct.us` decision corpus.
