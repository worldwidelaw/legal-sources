# US/MS-AGOpinions — Mississippi Attorney General Official Opinions

Full text of the official opinions issued by the **Mississippi Attorney
General**. Each opinion answers a legal question posed by a public
official (a county or municipal board, a state agency, a legislator) and
is an authoritative interpretation of Mississippi law — i.e. **doctrine**.

## Source

- Publisher: Office of the Mississippi Attorney General
- Index: <https://attorneygenerallynnfitch.com/divisions/opinions-and-policy/recent-opinions/>
- Documents: text-layer PDFs in the site's WordPress media library
  (`wp-content/uploads/YYYY/MM/...`)

## How it works

The "recent opinions" page is a single server-rendered HTML page that
lists **every published opinion since 2020** as a direct PDF link — no
JavaScript, no pagination, no CAPTCHA. The scraper:

1. Fetches the index page once and collects every
   `wp-content/uploads/*.pdf` link (~512 opinions).
2. Derives the issue date from the filename's `Month-DD-YYYY` pattern,
   falling back to the docket year (`YYYY-NNNNN` filenames) and finally
   the `/uploads/YYYY/MM/` media path.
3. Downloads each PDF via curl (the site's WAF 403s the default
   python-requests UA) and extracts its text layer (no OCR) via
   `common.pdf_extract`.
4. Takes the opinion title from its `Re:` subject line.

Coverage is **2020-present**. The pre-2020 historical corpus is only
available behind Westlaw (`govt.westlaw.com/msag`) and is **not**
included here.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (~512 opinions)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Mississippi Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
