# US/ME-AGOpinions — Maine Attorney General Opinions

Full text of formal opinions and memoranda issued by the **Maine Attorney
General** (1874–present). Each opinion answers a legal question posed by a
public official and is an authoritative interpretation of Maine law —
classified as **doctrine**.

## Source

- **Index:** https://www.maine.gov/legis/lawlib/lldl/agops/agmaster.html
- **PDF host:** `https://lldc.mainelegislature.org/Open/AG/Opinions/{year}/ag_*.pdf`
- **Publisher:** Maine Law and Legislative Digital Library (Maine State Law
  and Legislative Reference Library).

## Strategy

The digital library publishes a single server-rendered **master index** page
that lists every AG opinion in one HTML `<table>`. Each row carries the issue
date, opinion number, a one-line subject (the link text), the issuing Attorney
General, the requestor, and statutory/case citations, plus a direct link to a
text-recognised PDF.

1. `GET` the master index (one request) and parse every `<tr>`.
2. Keep rows whose link is `/Open/AG/Opinions/YYYY/ag_*.pdf`.
3. Download each PDF and extract its OCR text layer via `common.pdf_extract`.
4. Normalize into the standard doctrine schema (`text` = PDF body).

There is no pagination, no JavaScript, and no CAPTCHA. PDFs are OCR'd scans of
the original opinions ("Reproduced from scanned originals with text
recognition applied"); the first page is a library cover sheet and the opinion
body follows. A small minority of PDFs have no usable extractable text and are
skipped.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull (~9,100 opinions)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Coverage

- ~9,100 opinions and memoranda, 1874–present.
- Jurisdiction: US-ME (State of Maine).

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Maine Attorney General opinions are official state government works in the
public domain. Commercial use permitted; no attribution required.
