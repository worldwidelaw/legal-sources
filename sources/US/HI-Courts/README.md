# US/HI-Courts — Hawaii State Courts

**Source:** [https://courts.state.hi.us/](https://courts.state.hi.us/)
**Data types:** case_law
**Courts:** Supreme Court of Hawaii (`haw`), Intermediate Court of Appeals (`hawapp`)
**Corpus:** ~33,700 opinion records in CourtListener (1900s–present)

## Access

Opinions are discovered through CourtListener's **search API** (`/api/rest/v4/search/`,
`type=o`, cursor-paginated, no auth required). Full text is then resolved per opinion:

1. **CourtListener stored text** — `/api/rest/v4/opinions/{id}/`, taking the longest of
   `plain_text` / `html_with_citations` / `html_columbia` / `html_lawbox` /
   `html_anon_2020` / `html` / `xml_harvard`. One small JSON request per opinion, no PDF
   work. **Requires `COURTLISTENER_API_TOKEN`** (free, from a CourtListener account).
   This is the only path that reaches the ~1900–1990 Harvard/Columbia imports — those
   clusters carry no PDF at all, so a PDF-only crawl skips them silently.
2. **PDF fallback** — `storage.courtlistener.com` or the court's own site, extracted with
   PyMuPDF, deferring to the shared `extract_pdf_markdown` (opendataloader/pdfplumber/OCR)
   only when the fast path comes up short.

Records shorter than 500 characters are dropped: CourtListener stores thousands of Hawaii
table-of-dispositions stubs (`"Affirmed."`, `"Cert. denied"`) as opinion rows.

## Performance

Per-document text resolution runs inside `normalize()`, not `fetch_all()`, so
`bootstrap-fast`'s worker pool parallelises it. Throughput is bounded by CourtListener's
5,000 requests/hour authenticated throttle (`rate_limit` in `config.yaml`), giving a
full-corpus crawl of roughly 8 hours.

The search cursor is checkpointed to `data/checkpoint.json` after every page, so a killed
or timed-out run resumes at the next cursor instead of restarting. Use `--restart` to
discard the cursor and crawl from the beginning.

## Usage

```bash
export COURTLISTENER_API_TOKEN=...        # optional but strongly recommended
python bootstrap.py test                  # connectivity check
python bootstrap.py bootstrap --sample    # 15 sample records
python bootstrap.py bootstrap-fast        # full corpus, concurrent, resumable
python bootstrap.py update --since 2026-01-01
```

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
