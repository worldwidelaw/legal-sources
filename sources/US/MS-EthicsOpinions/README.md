# US/MS-EthicsOpinions — Mississippi Ethics Commission Advisory Opinions

Full text of the **Ethics Advisory Opinions** of the [Mississippi Ethics
Commission](https://www.ethics.ms.gov/), the independent state body that
administers the Ethics in Government Law (Miss. Code Ann. Title 25, Chapter 4)
and Section 109 of the Mississippi Constitution of 1890.

On request from a public servant, the Commission issues a written opinion
interpreting those provisions as applied to specific conduct. The opinion is
published (with the requester's identity edited out) and is the Commission's
authoritative interpretation of the ethics law — **doctrine**. About **1,542**
opinions (numbered `YY-NNN-E`, 2006–present).

## Access

No auth, no CAPTCHA. The Commission's opinion search is a server-side DataTable
backed by a public JSON endpoint:

- **List:** `GET https://www.ms.gov/msec/ethics/api/opinion/list?draw=1&start=0&length=5000`
  → `{data: [{id, documentId, number, summary, subjectTitleList}, ...]}` (all opinions in one call).
- **Detail:** `GET https://www.ms.gov/msec/ethics/opinion/details/{id}` → HTML linking the born-digital PDF.
- **Document:** `https://www.ms.gov/msec/ethics/Opinion/Document/{file}.pdf` (clean text layer).

The PDF filename is derived irregularly from the opinion number (the `-E` suffix
is dropped, `-ER` and others are kept), so the scraper reads the actual href from
the detail page. Full text is extracted via the shared `common.pdf_extract`
backend chain. The issue date is parsed from the `Month DD, YYYY` line in the PDF
body.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity + extraction test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — no restrictions.

Ethics advisory opinions of the Mississippi Ethics Commission are official public
records of a Mississippi state agency interpreting statute and the state
constitution (government-edict works), published for public use under the Ethics
in Government Law (Miss. Code Ann. Title 25, Chapter 4). Commercial use permitted;
no attribution required.
