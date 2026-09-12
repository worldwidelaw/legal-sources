# CA/YukonLaws — Yukon Consolidated Laws

**Source:** [https://laws.yukon.ca/cms/](https://laws.yukon.ca/cms/)
**Data types:** legislation
**Jurisdiction:** CA-YT (Yukon)

Consolidated Yukon legislation, published as PDFs under
`/cms/images/LEGISLATION/`:

| Collection    | Contents                          |
|---------------|-----------------------------------|
| `PRINCIPAL`   | Acts (incl. legacy `acts/`)       |
| `SUBORDINATE` | Regulations, OICs (legacy `regs/`)|
| `AMENDING`    | Amending acts                     |
| `GAZETTES`    | Yukon Gazette, Parts I & II       |
| `APPOINTMENT` | Appointment orders (legacy `app/`)|

## Access path — Internet Archive

The live host is **not reachable**. The whole yukon.ca legislation estate
(`laws.yukon.ca`, `legislation.yukon.ca`, `yukon.ca`) sits behind a Cloudflare
managed challenge: the by-title index POST, plain GETs and the
`/cms/images/LEGISLATION/` PDF assets all return `403` with
`server: cloudflare` / `cf-mitigated: challenge` and the "Just a moment..."
Turnstile interstitial, under both our own UA and `Chrome/126`, from a
residential vantage (re-verified 2026-08-18). It is neither a datacenter-IP
block nor a UA filter — the live site needs browser automation (issue #1441-D).

The corpus is therefore read from the Internet Archive:

1. **Enumerate** every archived legislation PDF via the Wayback CDX API over
   `laws.yukon.ca/cms/images/LEGISLATION/*.pdf`, keeping snapshots with
   status `200` or a revisit record (`-`).
2. **Fold the two URL schemes** the site has used into one document namespace:

   ```
   current  /LEGISLATION/{PRINCIPAL|SUBORDINATE|AMENDING|GAZETTES}/{year}/{id}/{id}[_N].pdf
   legacy   /LEGISLATION/{acts|regs|app}/{id}.pdf
   ```

   Documents are keyed on `(collection, doc_id)`; the current scheme wins over
   the legacy one, then the highest `_N` point-in-time version, then the newest
   snapshot.
3. **Replay** each PDF raw at `https://web.archive.org/web/{ts}id_/{url}`,
   walking back through older captures when a snapshot turns out to be an
   error body rather than a PDF, and extract the text with
   `common/pdf_extract`.
4. **Titles** are harvested from the archived HTML index pages
   (`acts-before-2003`, `acts-from-2003-onwards`, `acts-repealed`,
   `index-of-regulations`, the `annual-acts-*` articles, …), which link each PDF
   under its proper title, and cached to `data/wayback_titles.json`. The
   by-title index itself is POST-driven and unarchived, so anything those pages
   miss falls back to a title parsed from the PDF's own cover page.

Notes:

- A document folder may hold several versioned PDFs (`2002-0015_1.pdf`,
  `2002-0015_2.pdf`) for point-in-time consolidations. The highest-numbered
  version is the current consolidated text.
- Doc IDs are unique only *within* a collection — some `PRINCIPAL` ids are
  reused by unrelated `SUBORDINATE` documents. The bare id is kept in `_id`
  where it is unambiguous; collisions are qualified with the collection name.
- Year `0000` folders contain placeholder stubs ("This is a placeholder file
  only", or a pointer to a federal act) and are skipped.
- Yukon PDFs are bilingual two-column (English | French); the extracted text
  contains both language columns interleaved.

### Known coverage gap

`/LEGISLATION/historical_statutes/` (216 bound statute volumes, 1902–2002) and
`/LEGISLATION/ncnr/` (45 not-consolidated-not-repealed acts) are **scanned page
images with no text layer**. They are excluded from the crawl and reported via
`record_coverage_gap()` rather than ingested as empty records. Adding them
requires OCR (Tesseract), which the fleet image does not currently provide.

## Usage

```bash
python bootstrap.py test                 # CDX + replay connectivity check
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias used by the fleet runner
```

## License

[Open Government Licence — Canada](https://open.canada.ca/en/open-government-licence-canada) — attribution required, commercial use permitted.
