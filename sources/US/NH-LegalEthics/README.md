# US/NH-LegalEthics — New Hampshire Bar Association Ethics Opinions

Full text of the **Ethics Committee Advisory / Formal / Informal Opinions**
issued by the **New Hampshire Bar Association's Ethics Committee**. Each opinion
is the Committee's written interpretation of the **New Hampshire Rules of
Professional Conduct** in response to an inquiry about contemplated attorney
conduct, advising **lawyers**. The opinions are advisory — the Committee has no
disciplinary authority; lawyer discipline is administered by the N.H. Supreme
Court Attorney Discipline Office and Professional Conduct Committee.

- **Publisher:** New Hampshire Bar Association (the state's unified/mandatory bar)
- **Coverage:** bar-year numbered series `#YYYY-YY/N` (e.g. `#1990-91/1`,
  `#2017-18/01`), ~133 born-digital PDF opinions, **1970–present**
- **Type:** `doctrine` (advisory ethics opinions interpreting the NH RPC)
- **Full text:** yes — born-digital PDFs (PyMuPDF, no OCR)

## Source & method

- **Index:** opinions are enumerated from a set of public nhbar.org index pages
  — the master "list of all NHBA ethics opinions" page (1990–present) plus the
  per-decade pages (1970-1979; 1980-1989 / 1983-84-thru-1980; 1990-1999;
  2000-2007). Records are de-duplicated on the S3 PDF URL.
- **Two publishing schemes.** OLD (1970–1984): each opinion title links
  **directly** to a born-digital PDF on `nhba.s3.amazonaws.com`, with the
  `#YYYY-YY/N` number preceding the title in the page text (associated by a
  document-order walk). MODERN (1990–present): each opinion has a detail page
  (`/ethics/opinion-YYYY-YY-NN` or `/YYYY-YY-NN-slug/`) that shows only an
  **abstract**; its first **"Read More"** link is the full-text PDF. The scraper
  always resolves the "Read More" PDF so it captures the **full opinion body**,
  not the summary.
- **Full text:** each opinion is a born-digital PDF; extracted with PyMuPDF
  (`fitz`), **no OCR**. Records under 200 chars are skipped — a handful of the
  very oldest 1970s–80s opinions survive only as scanned images and are dropped.
- **Number:** canonical `YYYY-YY/N` (bar-year pair + sequence). The `_id` uses
  the number with `/`→`-`, falling back to the PDF filename stem.
- **Date:** an explicit `Month DD, YYYY` / `M/D/YY` date from the PDF body or
  index caption when present, else the first bar-year → `YYYY-01-01`.

No JavaScript, CAPTCHA or authentication is required.

## Distinct from

- **US/NH-Legislation** — New Hampshire statutes.
- **US/NH-Courts** — court decisions.

This is the attorney professional-conduct advisory-opinion series that in other
states is built as `US/{ST}-LegalEthics`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## License

[Public Domain / freely published advisory opinions](https://www.nhbar.org/list-of-all-nhba-ethics-opinions-and-articles-with-hyperlinks/) — no attribution required.

New Hampshire Bar Association Ethics Committee opinions are published free to the
public on nhbar.org as an educational service interpreting the New Hampshire
Rules of Professional Conduct. They are advisory and carry no login, paywall or
terms prohibiting reuse. Treated as effectively public domain, consistent with
the other state-bar legal-ethics sources. The New Hampshire Bar Association is
the state's unified (mandatory) bar, so the 17 U.S.C. § 105 government-edicts
rationale applies directly. Commercial use permitted.
