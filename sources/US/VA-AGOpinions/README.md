# US/VA-AGOpinions — Virginia Attorney General Official Opinions

Full text of official advisory opinions issued by the Attorney General of
Virginia under [Va. Code § 2.2-505](https://law.lis.virginia.gov/vacode/title2.2/chapter5/section2.2-505/).
Each opinion is the AG's authoritative interpretation of Virginia law,
issued at the request of the Governor, members of the General Assembly,
constitutional officers and other officials named in the statute. These are
official state government legal interpretations — classified as **doctrine**.

## Source

- **Publisher:** Office of the Attorney General of Virginia
- **Site:** https://www.oag.state.va.us/annual-reports-opinions/official-opinions
- **Coverage:** ~1,180 opinions online (2008-present plus a large 1990s/2000s backfill)
- **Auth:** none

## How it works

1. The listing page links a per-year "article" page (Joomla `view=article`)
   for every year; a few years carry more than one article page (e.g.
   `2023-official-opinions`, `2023-official-opinions-2`).
2. Each year page is server-rendered HTML linking every opinion's
   born-digital PDF at `/files/Opinions/{YEAR}/{file}.pdf`. The opinion
   number (`YY-NNN`) and the requester surname are encoded in the filename
   (e.g. `22-058-Youngkin-issued.pdf`).
3. `normalize()` downloads each PDF and extracts full text with
   `fitz`/PyMuPDF (Tesseract OCR fallback for the rare image-only scan).
   The issue date is parsed from the `Month DD, YYYY` line in the PDF body,
   falling back to the `/Opinions/{YEAR}/` folder year.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public Domain — 17 U.S.C. § 105 / Va. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — Virginia Attorney General official opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
