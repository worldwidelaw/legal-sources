# US/AK-LegalEthics — Alaska Bar Association Ethics Opinions

Full text of the **Ethics Opinions** adopted by the **Alaska Bar Association's
Ethics Committee / Board of Governors**, applying the Alaska Rules of
Professional Conduct (and, for the older opinions, the predecessor Code of
Professional Responsibility / Canons) to a stated question to advise **lawyers**
on the ethics of contemplated conduct. This is **doctrine** (advisory
interpretation of the rules governing attorneys).

- **Publisher:** Alaska Bar Association (the state's integrated/mandatory bar,
  operating under the authority of the Alaska Supreme Court)
- **Series:** per-year numbered `{year}-{N}` — older opinions use a two-digit
  year (`73-1`, `98-2`), opinions from 2000 on use a four-digit year (`2003-3`,
  `2012-3`, `2025-1`); ~150 opinions spanning 1968–present
- **Format:** born-digital PDFs (text layer) — extracted with PyMuPDF, no OCR
- **Jurisdiction:** US-AK

## Access / recipe

The `/ethics-discipline/ethics-opinions/` library landing page renders its
opinion list client-side (JavaScript), but the server-side **"Adopted Ethics
Opinions: Chronological"** page
(`/ethics-discipline/ethics-opinions/adopted-ethics-opinions-chronological/`)
carries every opinion as a plain anchor `<a href=".../wp-content/uploads/{file}.pdf">{number}</a>`.

The scraper parses that page. The anchor **text** is the authoritative opinion
number (`84-1`, `2025-1`); the **href** is the actual PDF (whose filename may
carry `-corr` / `-as-modified` / duplicate-upload suffixes the number does not,
e.g. `85-5-as-modified-by-2023-2.pdf`). A two-digit year `YY` is normalized to
`19YY`. This index-driven discovery captures non-contiguous numbers (e.g.
`84-9/10/11` after a gap, `76-8`, `95-6/7`) that a sequential `n=1..N` probe with
early stop-on-miss would silently skip. Each PDF header reads
`ALASKA BAR ASSOCIATION / ETHICS OPINION [NO.] {number}` followed by the subject
title and a Question/Issue/Facts block; the adoption date is the
`(Adopted|Approved) by (the) Board of Governors on <Month DD, YYYY>` line.

No CAPTCHA, no authentication, no OCR. A browser User-Agent is used.

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
```

## Distinct from

- **US/AK-Courts** — Alaska appellate court decisions
- **US/AK-Legislation** — Alaska Statutes / legislature
- **US/AK-OAH** — Alaska Office of Administrative Hearings (agency adjudications)

## License

[Public Domain (US government edict — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — no attribution required.

The Alaska Bar Association is the state's integrated (mandatory) bar operating
under the authority of the Alaska Supreme Court; its ethics opinions are the work
of a government-authorized body, treated as public domain under the government-
edicts rationale (consistent with the other integrated state-bar legal-ethics
sources). Published free to the public on alaskabar.org with no login, paywall,
or terms prohibiting reuse. **Commercial use permitted.**
