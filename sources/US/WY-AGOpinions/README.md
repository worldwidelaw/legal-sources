# US/WY-AGOpinions — Wyoming Attorney General Formal Opinions

Full text of the **Formal Opinions of the Wyoming Attorney General** —
authoritative interpretations of Wyoming law issued by the state's chief legal
officer. This is a **doctrine** corpus.

## Coverage

~19 documents, 1998–2018. The 1998 and 1999 entries are compiled multi-opinion
year volumes (one PDF holding that year's numbered opinions); the rest are one
numbered formal opinion per entry. 13 of the documents carry usable born-digital
text layers (9K–115K characters each); the remaining scanned/empty PDFs are
automatically skipped (no OCR is attempted).

## Access

The index page (`https://attorneygeneral.wyo.gov/formal-opinions`) is
server-rendered with no WAF or CAPTCHA. Each opinion links to a Google Drive
file, which is downloaded directly through the Drive `uc?export=download`
endpoint (the `resourcekey` is passed for the older `0B...`-format file IDs).
Full text is extracted with the shared, OOM-hardened
`common.pdf_extract.extract_pdf_markdown` helper.

## Usage

```bash
python3 bootstrap.py bootstrap --sample   # sample documents
python3 bootstrap.py bootstrap            # full pull
python3 bootstrap.py test-api             # connectivity / extraction test
```

## License

[Public Domain (US Government Work — Wyoming)](https://www.law.cornell.edu/uscode/text/17/105) — Wyoming Attorney General formal opinions are official State of Wyoming government works in the public domain. Commercial use permitted; no attribution required.
