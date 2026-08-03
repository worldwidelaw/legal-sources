# SL/Parliament — Sierra Leone Parliament: Acts of Parliament

Full-text Acts of Parliament of Sierra Leone, published by the national
Parliament on [parliament.gov.sl](https://www.parliament.gov.sl/acts.html).

## Source

The static index at `/acts.html` lists per-year pages
(`acts-{YEAR}-{NN}.html`) from 1920 to the present. Each year page links
directly to full-text Act PDFs at `/uploads/acts/{Title}.pdf`. The PDFs are
born-digital, so text extraction needs no OCR.

This is the official Parliament legislation source for Sierra Leone. It
supersedes reliance on the geo/IP-blocked SL/SierraLII LII aggregator.

## Method

1. `GET /acts.html` → collect each `acts-{YEAR}-{NN}.html` year page.
2. `GET` each year page → regex `/uploads/acts/*.pdf` hrefs.
3. Download each PDF and extract full text via `common.pdf_extract`.

`_type` = `legislation`. Enactment date is derived from the year in each
Act's title (e.g. "The Finance Act, 2024" → 2024-01-01).

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap --full     # full corpus
python bootstrap.py test                 # smoke test
```

## License

[Government Edict — Public Domain](https://www.parliament.gov.sl/) — Acts of
Parliament are government edicts of the Republic of Sierra Leone and are not
subject to copyright. Commercial use permitted.
