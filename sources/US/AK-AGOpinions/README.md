# US/AK-AGOpinions — Alaska Attorney General Formal Opinions

Full text of formal legal opinions issued by the **Alaska Attorney General**
(Alaska Department of Law). Each opinion answers a legal question posed by a
public official and constitutes an authoritative (advisory) interpretation of
Alaska law — classified as **doctrine**.

## Source

- **Publisher:** Alaska Department of Law (Office of the Attorney General)
- **Index:** https://law.alaska.gov/doclibrary/opinions-index/opinions_chron.html
- **Coverage:** 1990 (and older) – present
- **Format:** digitally-produced text PDFs (real text layer, no OCR needed)

## How it works

1. Fetch the chronological master index (`opinions_chron.html`) and extract the
   per-year index pages (`opinions{YYYY}.html`).
2. Parse each year page's `<li>` rows into `(issue date, PDF URL, title)`. PDF
   hrefs are relative (`../../pdf/opinions/opinions_{YYYY}/...`) and resolved
   against the year page URL.
3. Download each PDF and extract its text via the shared OOM-hardened
   `common.pdf_extract.extract_pdf_markdown` helper.
4. Normalize into the standard doctrine schema (`opinion_number`, `title`,
   `text`, `date`, `url`).

## Usage

```bash
python bootstrap.py test-api            # connectivity / extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all years)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Alaska)](https://www.law.cornell.edu/uscode/text/17/105) — Alaska Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
