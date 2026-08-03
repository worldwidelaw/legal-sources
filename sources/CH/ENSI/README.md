# CH/ENSI — Swiss Federal Nuclear Safety Inspectorate

ENSI (Eidgenössisches Nuklearsicherheitsinspektorat) is Switzerland's national
regulator for the nuclear safety and security of Swiss nuclear facilities. Under
the Nuclear Energy Act (KEG) it publishes binding regulatory guidelines
(**Richtlinien**, ENSI-A/B/G series) and requirements, plus expert reports
(Gutachten), advisory opinions (Stellungnahmen), safety-review assessments and
annual oversight reports (Aufsichtsberichte).

- **Guidelines / requirements** (Richtlinien, Anforderungen) → `legislation`
- **Everything else** (expert reports, opinions, oversight/annual reports) → `doctrine`

## Source

- **Document database:** https://www.ensi.admin.ch/de/dokumente/
- WordPress `document` post-type archive, paginated at `/de/dokumente/page/N/`.
  Each list item carries the document number, title, taxonomy categories and one
  or more direct PDF links under `/de/wp-content/uploads/...`. No per-document
  page visit is required; full text is extracted from the PDFs.

## How it works

1. `fetch_all()` walks the paginated document archive, parsing each
   `<li ... class="... document document-category-*">` entry inline (number,
   title, categories, PDF path, date from the `/uploads/YYYY/MM/` path).
2. `normalize()` downloads the PDF and extracts full text via the shared
   `common.pdf_extract` backend, tagging the record `legislation` when the
   document is in the `richtlinien`/`anforderungen` categories, else `doctrine`.

## Usage

```bash
python bootstrap.py test                # verify listing + one PDF download
python bootstrap.py bootstrap --sample  # fetch 15 sample records
python bootstrap.py bootstrap           # full run
```

## License

[Swiss Copyright Act, Art. 5 URG — official works](https://www.fedlex.admin.ch/eli/cc/1993/1798_1798_1798/de#art_5)
— ENSI is a Swiss federal regulatory body; its regulatory guidelines,
requirements and official reports are official works of the Confederation and
are not protected by copyright under Art. 5 URG. Reuse permitted with source
attribution. Commercial use permitted.
