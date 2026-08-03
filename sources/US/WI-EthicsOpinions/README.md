# US/WI-EthicsOpinions — Wisconsin Ethics Commission Formal Advisory Opinions

Full-text formal advisory opinions issued by the **Wisconsin Ethics Commission**
and its statutory predecessors — the **Wisconsin Ethics Board** and the
**Government Accountability Board (GAB)**.

Under Wis. Stat. § 19.46(2) the Commission issues formal advisory opinions
construing the state Code of Ethics for public officials (Wis. Stat. ch. 19,
subch. III), the lobbying law (ch. 13, subch. III) and the campaign-finance law
(ch. 11). Formal advisory opinions are public record and are the Commission's
official written interpretation of the ethics statutes = **doctrine**.

## Source

- **Listing:** <https://ethics.wi.gov/Pages/Resources/ResourcesOverview.aspx>
  (the Commission's "Resources" SharePoint document library, rendered as a
  single server-side HTML table)
- **Files:** `https://ethics.wi.gov/Resources/{filename}` — born-digital PDFs
  with a real text layer (no OCR, no CAPTCHA, no auth)
- **Corpus:** ~313 formal advisory opinions, 1974–present

## How it works

1. `GET` the Resources listing. Each document is one `<tr>` of
   `<td class="ms-vb2">` cells: `[modified date, audience, type, topic,
   filename, title]`. Rows whose **type** cell is `Opinion` are the formal
   advisory opinions.
2. Each `filename` resolves to a born-digital PDF at
   `/Resources/{filename}`; text is extracted via the shared
   `common.pdf_extract` backend.
3. The opinion number is parsed from the leading title token (`00-02`,
   `2008 GAB 03`, `04 Op. Eth Bd 103 (1981)`); the date is parsed from the
   opinion body (`Month DD, YYYY`) with a fallback to the year embedded in the
   number.

### User-Agent quirk

`ethics.wi.gov` serves the full server-rendered list table **only to
non-browser clients**. A Mozilla/browser User-Agent returns a JavaScript shell
with zero rows; a plain UA (`python-requests`/`curl`) returns all rows — the
same inversion seen on `mass.gov` (Akamai). The scraper sends a plain UA.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
```

## License

[Public Domain (State of Wisconsin Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
formal advisory opinions of the Wisconsin Ethics Commission are official public
records of the State of Wisconsin, published for public use with no copyright
restriction (government edict). Commercial use permitted; no attribution
required.
