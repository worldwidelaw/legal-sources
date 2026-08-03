# US/OR-AGOpinions — Oregon Attorney General Opinions

Full text of formal opinions issued by the **Oregon Attorney General**
(Oregon Department of Justice). Each opinion answers a legal question
posed by a state official and is an authoritative interpretation of
Oregon law (**doctrine**).

## Source

- **Publisher:** Oregon Department of Justice — Office of the Attorney General
- **Index:** https://www.doj.state.or.us/oregon-department-of-justice/office-of-the-attorney-general/attorney-general-opinions/
- **Coverage:** 1997–present
- **Format:** Text-layer PDFs (one per opinion)

## How it works

The opinions index is a single static, server-rendered HTML page. Each
opinion is published as a structured block carrying its designation
(e.g. `OP-2013-2`), the requesting official/agency, the issue date, a
one-line topic summary, and a direct link to the full-text PDF under
`/wp-content/uploads/YYYY/MM/*.pdf`. No pagination, JavaScript, or
CAPTCHA is involved.

The scraper:

1. Fetches the index page once and parses every opinion block.
2. Downloads each PDF and extracts its text layer via `common.pdf_extract`.
3. Skips the minority of recent opinions that are **image-only scans**
   (0 extractable characters) pending OCR.
4. Normalizes into the standard doctrine schema (full `text`, ISO `date`).

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Oregon Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
