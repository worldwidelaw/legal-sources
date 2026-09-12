# VN/PhapLuatGov — Vietnam National Legal Document Database (Hệ thống văn bản)

**Source:** [https://vanban.chinhphu.vn/](https://vanban.chinhphu.vn/)
**Data types:** legislation

Central-level Vietnamese legal instruments — constitutions, laws, ordinances,
decrees, decisions, circulars and joint circulars — published by the Government
Office. The listing reports ~47,700 documents under `classid=1`
(văn bản quy phạm pháp luật).

## Access path

| Step | Endpoint |
|------|----------|
| Listing | `/he-thong-van-ban?classid=1&mode=1` — ASP.NET GridView, 50 rows per page |
| Paging | `__doPostBack('…$grvDocument', 'Page$N')` POST carrying `__VIEWSTATE` |
| Detail | `/?pageid=27160&docid={docid}&classid=1` |
| Full text | the signed attachment on `https://datafiles.chinhphu.vn/cpp/files/vbpq/…` |

Each listing row already carries the document number, issue date, title and
attachment URLs, so the detail page is not fetched during a full crawl.

Full text is extracted from the attachment: PDFs through `common.pdf_extract`,
legacy `.doc` through `common.doc_extract` (Windows-1258 code page for
Vietnamese), `.docx` through `python-docx`, and `.rtf` through `striprtf`.

### Gotchas

- `page=`, `pageindex=` and `maxresults=` are accepted with HTTP 200 and then
  **silently ignored** — the grid always returns page 1. Real paging is the
  WebForms postback above; assert the echoed `document_page_info` counter
  ("51 - 100 | 47710") when changing this code.
- The listing walk must run in order, because each response carries the
  `__VIEWSTATE` needed for the next page (a `Page$500` postback built from page
  1's viewstate returns HTTP 200 with an empty grid, not page 500).
  `walk_index()` is therefore consumed lazily — each page's documents are
  fetched as that page arrives, so an interrupted crawl keeps everything
  written so far — and it caches to `data/index.json` behind a `complete` flag
  so a truncated walk is never mistaken for the whole corpus.
- Roughly half the attached PDFs are image-only scans whose sole text layer is
  the digital-signature stamp, so they reach the OCR fallback. `PDF_OCR_LANG`
  is defaulted to `vie`; on a host without a Vietnamese tesseract language pack
  those documents are dropped by the `is_vietnamese_text()` gate rather than
  ingested as mojibake.
- Document numbers occasionally contain a Cyrillic Ер (`Р`) instead of a Latin
  `P` (e.g. `NĐ-CР`), straight from the publisher's HTML.

### History

Rebuilt in 2026-08 for issue #1445. The original path read the Ministry of
Justice portal at `vbpl.vn`, which now serves a site-wide JavaScript security
challenge (`_jsc_ch_conf` / `ws_sec_page.js`) and returns HTTP 403 to every
non-browser client — verified from residential and datacenter vantages and
across four user agents. `vanban.chinhphu.vn` carries the same corpus as plain
server-rendered HTML.

Because the host changed, record ids moved from `VN-VBPL-{ItemID}` to
`VN-VBCP-{docid}`; the previously ingested rows are a distinct id space.

## Usage

```bash
python3 bootstrap.py bootstrap --sample   # 15 validation records into sample/
python3 bootstrap.py bootstrap-fast       # full corpus -> data/records.jsonl
python3 bootstrap.py update --since 2026-01-01
```

## License

[Public domain — official state acts](https://vanban.chinhphu.vn/) — Vietnamese
legal documents are official state texts and are excluded from copyright
protection by Article 15.1 of the Law on Intellectual Property (Luật Sở hữu trí
tuệ). Commercial use permitted; attribution to the publisher is courteous but
not legally required.
