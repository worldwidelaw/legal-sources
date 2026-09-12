# GM/LawHubGambia — Law Hub Gambia

**Source:** [https://www.lawhubgambia.com/](https://www.lawhubgambia.com/)
**Data types:** case_law, legislation

Law Hub Gambia is a not-for-profit database providing free public access to
Gambian national legislation and judgments. Case law and the 1997 Constitution
are published as HTML pages; legislation is published as PDFs, most of which are
**scans of printed acts** rather than born-digital text.

## Extraction notes

Every republished PDF carries a per-page watermark (`Law Hub Gambia Digital`,
`Document Sourced from www.lawhubgambia.com`). On the image-only scans that
watermark is the *entire* text layer, which used to be stored as if it were the
document — a 142-page bill came through as 3,450 characters of repeated stamp,
and the Elections (Amendment) Act as 46 characters (issue #1414). The scraper
now:

- de-watermarks extracted text and strips the LLMC scanning preamble on the
  digitised colonial volumes, plus stray Markdown heading/image artifacts;
- refuses to emit a record under `MIN_DOC_CHARS`, so a scan OCR cannot read is
  skipped loudly instead of stored as a stamp;
- raises the shared OCR page cap (tuned for short court PDFs) so long acts are
  not truncated a third of the way through;
- runs tesseract in `--psm 4` (`PDF_OCR_PSM`). These acts set their
  arrangement-of-sections as a narrow column of section numbers beside a column
  of headings; tesseract's default page analysis reads that as two independent
  blocks and emits a run of bare numbers followed by a run of bare headings,
  which is the "fragmented arrangement-of-sections" of issue #1414. psm 4 treats
  the page as a single column and keeps each number with its heading. On
  single-column body pages the two modes are byte-identical.

De-watermarking also has to survive OCR. The stamp reaches us as an image, so
tesseract puts the page number, the rule under the stamp and the bleed-through
from the facing page on the stamp's own line — `Law Hub Gambia Digital : 7 Oo —`.
A line-anchored pattern never matches those, so the scraper drops any line
carrying the stamp whose remainder holds no word.

The matching fix in `common/pdf_extract.py` applies to every source: the test
for "is this a real text layer?" was a flat 100-character whole-document floor,
which a stamp repeated over 142 pages clears easily, so the scan short-circuited
the backend chain and never reached OCR. The floor now scales with the page
count (`PDF_MIN_CHARS_PER_PAGE`, default 50).

The reported duplicate section numbers were not an upstream defect. The 1934
LLMC volume binds *two* ordinances — the Criminal Code (Act No. 25 of 1933) and
the Criminal Procedure Code (Act No. 26 of 1933), each numbered from 1 — and
storing it whole put both in one record. `COMPILED_PDFS` now cuts it at the
Criminal Procedure Code's title page into `criminal-code-ordinance-1934` and
`criminal-procedure-code-ordinance-1934`. These are the original 1933/34
ordinance texts; `criminal-code-1933` and `criminal-procedure-code-1933` are the
later consolidated Chapter 10:01 / 12:01 revisions of the same acts, so both
editions are kept.

Documents that *do* carry a text layer are not re-OCR'd. A/B'd against tesseract
at 300 DPI on the 1934 ordinance, the embedded layer is the equal or better of
the two, so re-OCR would cost hours of fleet time to trade one class of
character error for another.

## Usage

```bash
python3 bootstrap.py test-api             # connectivity check
python3 bootstrap.py bootstrap --sample   # 15 sample records
python3 bootstrap.py bootstrap            # full pull
python3 bootstrap.py bootstrap-fast       # full pull, concurrent (fleet entry point)
```

Requires `tesseract` for the scanned acts; without it the image-only PDFs are
skipped rather than stored as watermark text.

## License

[Terms of Use](https://www.lawhubgambia.com/terms-of-use) — Law Hub Gambia
publishes Gambian statutes and judgments as a free public-access, not-for-profit
database and expressly disclaims copyright ownership over those source documents
("Law Hub Gambia is not the copyright owner of statutes and case law published
on the website"). The underlying material is Gambian government legislation and
court judgments. Attribution to the publisher is appropriate.
