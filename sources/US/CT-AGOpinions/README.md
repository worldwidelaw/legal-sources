# US/CT-AGOpinions — Connecticut Attorney General Formal Opinions

Full text of formal legal opinions issued by the **Connecticut Attorney
General** (Office of the Attorney General). Each opinion answers a legal
question posed by a public official and constitutes an authoritative (advisory)
interpretation of Connecticut law — classified as **doctrine**.

## Source

- **Publisher:** Connecticut Office of the Attorney General
- **Hub:** https://portal.ct.gov/ag/opinions
- **Coverage:** 1990 – present
- **Format:** digitally-produced text PDFs (a few older scanned ones are skipped)

## How it works

1. Fetch the opinions hub (`/ag/opinions`) and extract the per-year index pages
   (`/ag/opinions/{YYYY}-formal-opinions`).
2. Parse each year page's anchors to `/-/media/ag/opinions/.../*.pdf` →
   `(PDF URL, descriptive title)`. The subject comes from the anchor `title=`
   attribute; an unrelated accessibility-policy PDF is filtered out by path.
3. Download each PDF and extract its text via the shared OOM-hardened
   `common.pdf_extract.extract_pdf_markdown` helper.
4. Derive the issued date from the opinion's opening lines; normalize into the
   standard doctrine schema (`opinion_number`, `title`, `text`, `date`, `url`).

## Usage

```bash
python bootstrap.py test-api            # connectivity / extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all years)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work — Connecticut)](https://www.law.cornell.edu/uscode/text/17/105) — Connecticut Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
