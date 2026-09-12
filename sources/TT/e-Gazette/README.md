# TT/e-Gazette — Trinidad & Tobago Official Gazette (Government Printery)

Collects full-text **legislation** from the Republic of Trinidad & Tobago's
official e-Gazette, published by the Government Printery at
<https://printery.gov.tt/e-gazette/>.

## What it collects

- **Acts of Parliament** (enacted laws and bills) — `Acts/` folder per year
- **Legal Notices** (statutory instruments / subsidiary legislation) — `Legal Notices/` folder per year

Documents are published as per-document, digital-native (text-extractable) PDFs
organized in an open Apache directory listing by year. The scraper walks the
year index, lists the `Acts` and `Legal Notices` folders, downloads each PDF,
and extracts the full text. PDFs yielding fewer than 400 characters of
extractable text (e.g. older scanned compilations) are skipped — no OCR.

## One record per instrument

A dozen files in the `Legal Notices` folders carry a range in their name
(`Legal Notice No. 177-190 of 2023.pdf`) and hold a whole Legal Supplement —
up to 32 separate statutory instruments in one PDF. Stored whole they cannot be
segmented, because every instrument restarts its own section numbering inside
the same "document". Those files are split on their `LEGAL NOTICE NO. n`
headers into one record per notice, each keyed
`TT-GAZ-Legal-Notice-No-{n}-of-{year}` — the same shape a standalone notice
gets, so a notice does not change identity depending on how it was published.
Split notices take their `date` from the Legal Supplement header (the gazette
publication date) rather than the first date in the body, which for land
acquisition orders is usually a much older survey date.

## Overlaid duplicate text

Some Acts (Act No. 2 of 2026 among them) draw their schedule pages twice at a
slight offset. The two copies fall inside pdfplumber's line-merge tolerance, so
their glyphs sort together and the line comes out interleaved
(`ItIetmem FIFRIRSTS TC COOLULUMMNN`). Extraction detects that signature per
page and drops the redundant copy first. Pages with no exact repeated draw run
are extracted exactly as before, byte for byte.

## Access method

No public API. Open directory listing (HTML) + per-document PDF download.
Rate limited to ~1 request/second.

## Fields

`_id`, `_source`, `_type`, `_fetched_at`, `title`, `text` (full document body),
`date`, `url`, `pdf_url`, `document_number`, `category`, `year`, `language`,
`jurisdiction`. Records split out of a bundled supplement also carry
`bundle_filename`.

## License

[Open Government Data](https://printery.gov.tt/) — Acts of Parliament and Legal
Notices of Trinidad & Tobago are official government legal texts published
openly by the Government Printery without registration. No explicit
machine-readable license is stated; treated as open government data (public
legal texts). Commercial use permitted.
