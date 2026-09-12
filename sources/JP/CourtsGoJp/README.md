# JP/CourtsGoJp — Japanese Courts Case Law Database

**Source:** [https://www.courts.go.jp/app/hanrei_jp/search1](https://www.courts.go.jp/app/hanrei_jp/search1)
**Data types:** case_law

Official judgments of the Supreme Court (最高裁判例), High Courts (高裁判例) and
lower courts (下級裁裁判例), with full text extracted from the published PDFs.
Roughly 50,000 judgments (~29,400 Supreme Court, ~4,900 High Court, ~15,400 lower
courts).

## How it works

1. **Enumerate** — the search endpoints (`/hanrei/search2` for Supreme/High,
   `/hanrei/search4` for lower courts) accept an arbitrary `limit`, so the whole
   corpus is listed in ~50 requests at 1,000 rows per page. The result rows
   already carry the case number, case name, judgment date, court, judgment type,
   outcome and PDF URL, so the per-case detail page is never fetched.
   Both `filter[judgeDateFrom]` and `filter[judgeDateTo]` must be present or the
   endpoint returns the landing page instead of running the query.
2. **Fetch** — `normalize()` downloads the judgment PDF and extracts its text, so
   `bootstrap-fast` overlaps the downloads across worker threads.

## Incremental / resumable crawl

State lives in `data/` next to the module (git-ignored):

- `processed_ids.txt` — every case ID whose record has been written. Cases in
  this log are skipped before their PDF is downloaded, so a refresh costs only
  the ~50 search requests plus the PDFs of genuinely new judgments.
- `checkpoint.json` — per-court enumeration offset. An interrupted run resumes at
  the last completed page; once every court is done the next run starts a fresh
  pass from offset 0 to pick up newly published judgments.

On startup the resume log is reconciled against the storage index, so cases that
were marked processed while their batch was still in flight (a kill mid-batch)
are re-queued rather than lost.

## Usage

```bash
python bootstrap.py test                 # connectivity + one PDF extraction
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap-fast       # concurrent full pull (fleet entry point)
python bootstrap.py update --since 2024-01-01
```

## License

[Open government data](https://www.e-gov.go.jp) — Japanese government content
published under the standard terms of use (reuse permitted, including
commercially, with attribution).
