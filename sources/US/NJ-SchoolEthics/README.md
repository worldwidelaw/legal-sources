# US/NJ-SchoolEthics — New Jersey School Ethics Commission Advisory Opinions

Full text of the public advisory opinions of the **New Jersey School Ethics
Commission (SEC)** (in the Department of Education), interpreting the **School
Ethics Act** (N.J.S.A. 18A:12-21 et seq.) — the conflict-of-interest and
prohibited-acts code for local board of education members and school
administrators. Each public advisory opinion is the Commission's authoritative
written interpretation, released for public use → `doctrine`.

> Distinct from the NJ *State* Ethics Commission (browser-bound JS database),
> `US/NJ-PERC` and `US/NJ-OAL`.

## Source & access

`www.nj.gov` serves a single server-rendered index (plain GET, no auth, no
CAPTCHA, no JS engine required).

1. **Enumeration:** the index
   `https://www.nj.gov/education/legal/ethics/advisory/` links every opinion
   PDF. Two path shapes: recent opinions under `/advisory/{YYYY}/`, older
   opinions under `/advisory/cat{1-7}/` (grouped by subject). Filenames are
   irregular (`A13-20.pdf`, `a2804pub.pdf`, `a0198opn.pdf`), so the href is read
   directly from each anchor and the opinion number is taken from the anchor
   text (`A12-26`), falling back to the filename.
2. **Full text:** each PDF is born-digital (clean text layer; OCR fallback via
   `common.pdf_extract` for any scan). The issue date is parsed from the
   `Month DD, YYYY` line near the top of the opinion.

~187 public advisory opinions (1998–present).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~187 opinions)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
public advisory opinions of the New Jersey School Ethics Commission are official
public records of the State of New Jersey, released for public use with no
copyright restriction. Commercial use permitted.
